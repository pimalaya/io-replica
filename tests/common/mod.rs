//! Shared in-memory storage and fake remote for the std-client tests.
//!
//! The in-memory backends prove the engine drives the protocol purely
//! through its `Wants`, knowing nothing of either side. The fake remote
//! models both backend families: with `mutable` unset it behaves like
//! IMAP (no content revisions), with it set like WebDAV (per-item
//! revisions, optimistic-concurrency rejections on stale if_match). It
//! also serves real delta snapshots from a numeric checkpoint, with
//! explicit vanished tracking, so incremental sync paths run end to end.

use std::{collections::BTreeMap, convert::Infallible};

use io_replica::{
    change::{ReplicaChange, ReplicaWriteOp},
    client::{ReplicaRemote, ReplicaStorage},
    collection::{ReplicaCheckpoint, ReplicaCollectionId},
    object::{ReplicaHash, ReplicaObject},
    placement::{ReplicaFlags, ReplicaHandle, ReplicaLinkId, ReplicaMeta, ReplicaPlacement},
    remote::{
        ReplicaFetchedItem, ReplicaPushOutcome, ReplicaPushResult, ReplicaRemoteItem,
        ReplicaRemoteSnapshot, ReplicaTier,
    },
    storage::ReplicaLoaded,
};

// ---- in-memory storage ------------------------------------------------

#[derive(Default)]
pub struct MemStorage {
    pub placements: BTreeMap<(ReplicaCollectionId, ReplicaHandle), ReplicaPlacement>,
    pub objects: BTreeMap<ReplicaHash, (ReplicaObject, Vec<u8>)>,
    pub checkpoints: BTreeMap<ReplicaCollectionId, ReplicaCheckpoint>,
}

impl MemStorage {
    pub fn placement(&self, collection: &str, handle: &str) -> &ReplicaPlacement {
        self.placements
            .get(&(collection.into(), ReplicaHandle::from(handle)))
            .expect("placement exists")
    }
}

impl ReplicaStorage for MemStorage {
    type Error = Infallible;

    fn load(&self, collection: &ReplicaCollectionId) -> Result<ReplicaLoaded, Infallible> {
        let placements = self
            .placements
            .iter()
            .filter(|((c, _), _)| c == collection)
            .map(|(_, p)| p.clone())
            .collect();
        let checkpoint = self.checkpoints.get(collection).cloned();

        Ok(ReplicaLoaded {
            placements,
            checkpoint,
        })
    }

    fn lookup_objects(
        &self,
        links: &[ReplicaLinkId],
    ) -> Result<BTreeMap<ReplicaLinkId, ReplicaHash>, Infallible> {
        let mut known = BTreeMap::new();

        for link in links {
            // any placement, in any collection, that already links this
            // logical item to a stored body
            let hit = self.placements.values().find_map(|p| {
                let matches = p.link_id.as_ref() == Some(link);
                matches.then(|| p.object.clone()).flatten()
            });
            if let Some(hash) = hit {
                known.insert(link.clone(), hash);
            }
        }

        Ok(known)
    }

    fn write(&mut self, ops: Vec<ReplicaWriteOp>) -> Result<(), Infallible> {
        for op in ops {
            match op {
                ReplicaWriteOp::UpsertPlacement(p) => {
                    self.placements
                        .insert((p.collection.clone(), p.handle.clone()), p);
                }
                ReplicaWriteOp::DropPlacement { collection, handle } => {
                    self.placements.remove(&(collection, handle));
                }
                ReplicaWriteOp::StoreObject { object, body } => {
                    self.objects.insert(object.hash.clone(), (object, body));
                }
                ReplicaWriteOp::SetCheckpoint {
                    collection,
                    checkpoint,
                } => {
                    self.checkpoints.insert(collection, checkpoint);
                }
            }
        }

        Ok(())
    }
}

// ---- fake remote ------------------------------------------------------

#[derive(Clone)]
pub struct ServerItem {
    pub link_id: ReplicaLinkId,
    pub flags: ReplicaFlags,
    pub body: Vec<u8>,
    /// The global change counter value of the item's last change; drives
    /// delta snapshots.
    pub seq: usize,
    /// The content revision, bumped on every content change; reported
    /// only when the remote is `mutable`.
    pub rev: usize,
}

#[derive(Default)]
pub struct MemRemote {
    pub items: BTreeMap<ReplicaCollectionId, BTreeMap<ReplicaHandle, ServerItem>>,
    pub full_fetches: Vec<ReplicaHandle>,
    pub calls: usize,
    /// The handle assigned to the next accepted append (an add with no
    /// origin); incremented per append.
    pub next_appended: usize,
    /// When true the remote reports content revisions (a WebDAV etag) and
    /// rejects pushes whose if_match is stale; when false it behaves like
    /// an immutable-content backend (IMAP) and reports none.
    pub mutable: bool,
    /// The global change counter; every mutation bumps it, and a delta
    /// snapshot serves everything past the cursor's value.
    seq: usize,
    /// Handles removed, stamped with the counter value of their removal.
    vanished: BTreeMap<ReplicaCollectionId, Vec<(usize, ReplicaHandle)>>,
}

impl MemRemote {
    fn bump(&mut self) -> usize {
        self.seq += 1;
        self.seq
    }

    fn revision(&self, rev: usize) -> Option<String> {
        self.mutable.then(|| rev.to_string())
    }

    pub fn seed(
        &mut self,
        collection: &str,
        handle: &str,
        link: &str,
        flags: &[&str],
        body: &[u8],
    ) {
        let seq = self.bump();
        let collection = self.items.entry(collection.into()).or_default();
        // re-seeding an existing handle is a content change, never a
        // revision regression
        let rev = collection
            .get(&ReplicaHandle::from(handle))
            .map(|i| i.rev + 1)
            .unwrap_or(0);

        collection.insert(
            ReplicaHandle::from(handle),
            ServerItem {
                link_id: ReplicaLinkId::from(link),
                flags: ReplicaFlags::from_iter(flags.iter().copied()),
                body: body.to_vec(),
                seq,
                rev,
            },
        );
    }

    pub fn set_flags(&mut self, collection: &str, handle: &str, flags: &[&str]) {
        let seq = self.bump();
        let item = self
            .items
            .get_mut(&collection.into())
            .and_then(|c| c.get_mut(&ReplicaHandle::from(handle)))
            .expect("server item exists");
        item.flags = ReplicaFlags::from_iter(flags.iter().copied());
        item.seq = seq;
    }

    /// A server-side content edit: the revision advances.
    pub fn edit(&mut self, collection: &str, handle: &str, body: &[u8]) {
        let seq = self.bump();
        let item = self
            .items
            .get_mut(&collection.into())
            .and_then(|c| c.get_mut(&ReplicaHandle::from(handle)))
            .expect("server item exists");
        item.body = body.to_vec();
        item.seq = seq;
        item.rev += 1;
    }

    pub fn remove(&mut self, collection: &str, handle: &str) {
        let seq = self.bump();
        let collection = ReplicaCollectionId::from(collection);
        self.items
            .get_mut(&collection)
            .and_then(|c| c.remove(&ReplicaHandle::from(handle)))
            .expect("server item exists");
        self.vanished
            .entry(collection)
            .or_default()
            .push((seq, ReplicaHandle::from(handle)));
    }

    /// A handle-space change (an IMAP UIDVALIDITY bump): every member is
    /// renumbered onto a fresh handle, contents untouched. Returns the
    /// old-to-new handle mapping.
    #[allow(dead_code)]
    pub fn renumber(
        &mut self,
        collection: &str,
        generation: usize,
    ) -> BTreeMap<ReplicaHandle, ReplicaHandle> {
        let seq = self.bump();
        let members = self.items.entry(collection.into()).or_default();
        let mut mapping = BTreeMap::new();

        let old = std::mem::take(members);
        for (index, (handle, mut item)) in old.into_iter().enumerate() {
            let new = ReplicaHandle::from(format!("v{generation}-{index}"));
            item.seq = seq;
            mapping.insert(handle, new.clone());
            members.insert(new, item);
        }

        mapping
    }

    pub fn flags_of(&self, collection: &str, handle: &str) -> &ReplicaFlags {
        &self.item(collection, handle).flags
    }

    pub fn rev_of(&self, collection: &str, handle: &str) -> usize {
        self.item(collection, handle).rev
    }

    fn item(&self, collection: &str, handle: &str) -> &ServerItem {
        self.items
            .get(&collection.into())
            .and_then(|c| c.get(&ReplicaHandle::from(handle)))
            .expect("server item exists")
    }
}

pub fn hash(body: &[u8]) -> ReplicaHash {
    // tiny deterministic content hash: identical bytes collapse to one
    // object, which is exactly what the dedup path keys on
    let mut acc: u64 = 1469598103934665603;
    for byte in body {
        acc ^= *byte as u64;
        acc = acc.wrapping_mul(1099511628211);
    }
    ReplicaHash::from(format!("{acc:016x}"))
}

impl ReplicaRemote for MemRemote {
    type Error = Infallible;

    fn enumerate(
        &mut self,
        collection: &ReplicaCollectionId,
        cursor: Option<ReplicaCheckpoint>,
    ) -> Result<ReplicaRemoteSnapshot, Infallible> {
        self.calls += 1;
        let checkpoint = ReplicaCheckpoint(self.seq.to_string().into_bytes());

        // a cursor that parses as a counter value yields a delta from it;
        // anything else falls back to a complete snapshot
        let since = cursor
            .as_ref()
            .and_then(|c| std::str::from_utf8(&c.0).ok())
            .and_then(|s| s.parse::<usize>().ok());

        let members = self.items.get(collection);

        let (items, vanished, complete) = match since {
            Some(since) => {
                let items = members
                    .into_iter()
                    .flatten()
                    .filter(|(_, item)| item.seq > since)
                    .map(|(handle, item)| ReplicaRemoteItem {
                        handle: handle.clone(),
                        flags: item.flags.clone(),
                        revision: self.revision(item.rev),
                    })
                    .collect();
                // NOTE: a vanished entry is dropped when the handle exists
                // again (a real server never reuses handles; the fake only
                // reports the current truth)
                let vanished = self
                    .vanished
                    .get(collection)
                    .into_iter()
                    .flatten()
                    .filter(|(seq, handle)| {
                        *seq > since && !members.is_some_and(|c| c.contains_key(handle))
                    })
                    .map(|(_, handle)| handle.clone())
                    .collect();
                (items, vanished, false)
            }
            None => {
                let items = members
                    .into_iter()
                    .flatten()
                    .map(|(handle, item)| ReplicaRemoteItem {
                        handle: handle.clone(),
                        flags: item.flags.clone(),
                        revision: self.revision(item.rev),
                    })
                    .collect();
                (items, Vec::new(), true)
            }
        };

        Ok(ReplicaRemoteSnapshot {
            items,
            vanished,
            complete,
            checkpoint,
        })
    }

    fn fetch(
        &mut self,
        collection: &ReplicaCollectionId,
        handles: Vec<ReplicaHandle>,
        tier: ReplicaTier,
    ) -> Result<Vec<ReplicaFetchedItem>, Infallible> {
        self.calls += 1;

        let collection = self.items.get(collection).cloned().unwrap_or_default();
        let mut out = Vec::new();

        for handle in handles {
            // an unknown handle yields nothing, like a real server
            let Some(item) = collection.get(&handle) else {
                continue;
            };
            let meta = ReplicaMeta(format!("headers:{}", handle.as_str()));

            let body = match tier {
                ReplicaTier::Meta => None,
                ReplicaTier::Full => {
                    self.full_fetches.push(handle.clone());
                    Some((hash(&item.body), item.body.clone()))
                }
            };

            out.push(ReplicaFetchedItem {
                handle,
                link_id: item.link_id.clone(),
                meta,
                body,
                revision: self.revision(item.rev),
            });
        }

        Ok(out)
    }

    fn push(
        &mut self,
        collection: &ReplicaCollectionId,
        changes: Vec<ReplicaChange>,
    ) -> Result<Vec<ReplicaPushResult>, Infallible> {
        self.calls += 1;
        let mut results = Vec::new();

        for change in changes {
            let result = match change {
                ReplicaChange::SetFlags { handle, flags } => {
                    let seq = self.bump();
                    if let Some(item) = self
                        .items
                        .get_mut(collection)
                        .and_then(|c| c.get_mut(&handle))
                    {
                        item.flags = flags;
                        item.seq = seq;
                    }
                    accepted(handle, None, None)
                }
                ReplicaChange::Remove {
                    handle,
                    to,
                    if_match,
                } => {
                    let stale = self.mutable
                        && self
                            .items
                            .get(collection)
                            .and_then(|c| c.get(&handle))
                            .is_some_and(|item| {
                                if_match.as_deref() != Some(item.rev.to_string().as_str())
                            });
                    if stale {
                        // the item changed since the delete was staged:
                        // optimistic concurrency protects it
                        rejected(handle)
                    } else {
                        let seq = self.bump();
                        // a move relocates the item; a plain delete drops
                        // it; a missing item means the delete already
                        // landed, which is an accept by contract
                        if let Some(item) = self
                            .items
                            .get_mut(collection)
                            .and_then(|c| c.remove(&handle))
                        {
                            self.vanished
                                .entry(collection.clone())
                                .or_default()
                                .push((seq, handle.clone()));
                            if let Some(target) = to {
                                let moved =
                                    ReplicaHandle::from(format!("{}-moved", handle.as_str()));
                                let mut item = item;
                                item.seq = seq;
                                self.items.entry(target).or_default().insert(moved, item);
                            }
                        }
                        accepted(handle, None, None)
                    }
                }
                ReplicaChange::Update {
                    handle,
                    object,
                    if_match,
                } => {
                    let Some(item) = self
                        .items
                        .get_mut(collection)
                        .and_then(|c| c.get_mut(&handle))
                    else {
                        // deleted meanwhile: the edit cannot land in place
                        results.push(rejected(handle));
                        continue;
                    };
                    let stale =
                        self.mutable && if_match.as_deref() != Some(item.rev.to_string().as_str());
                    if stale {
                        rejected(handle)
                    } else {
                        item.body = object.as_str().as_bytes().to_vec();
                        item.rev += 1;
                        let rev = item.rev;
                        let seq = self.bump();
                        let item = self
                            .items
                            .get_mut(collection)
                            .and_then(|c| c.get_mut(&handle))
                            .expect("just updated");
                        item.seq = seq;
                        let revision = self.revision(rev);
                        accepted(handle, None, revision)
                    }
                }
                // A create: server-side copy the origin item (COPYUID), or
                // append the uploaded body under a fresh handle. A copy
                // whose origin is gone is rejected (a real server errors
                // on copying an expunged message).
                ReplicaChange::Add {
                    handle,
                    link_id,
                    flags,
                    origin,
                    object,
                } => {
                    let assigned = match origin {
                        Some(o) => {
                            let item = self
                                .items
                                .get(&o.collection)
                                .and_then(|c| c.get(&o.handle))
                                .cloned();
                            let Some(mut item) = item else {
                                results.push(rejected(handle));
                                continue;
                            };
                            let seq = self.bump();
                            let new = ReplicaHandle::from(format!("{}-copy", o.handle.as_str()));
                            item.seq = seq;
                            self.items
                                .entry(collection.clone())
                                .or_default()
                                .insert(new.clone(), item);
                            Some(new)
                        }
                        None => object.as_ref().map(|object| {
                            let seq = self.bump();
                            self.next_appended += 1;
                            let new = ReplicaHandle::from(format!("app-{}", self.next_appended));
                            let link = link_id
                                .clone()
                                .unwrap_or_else(|| ReplicaLinkId::from(new.as_str()));
                            self.items.entry(collection.clone()).or_default().insert(
                                new.clone(),
                                ServerItem {
                                    link_id: link,
                                    flags: flags.clone(),
                                    body: object.as_str().as_bytes().to_vec(),
                                    seq,
                                    rev: 0,
                                },
                            );
                            new
                        }),
                    };
                    let revision = assigned
                        .as_ref()
                        .and_then(|new| self.items.get(collection)?.get(new))
                        .map(|item| item.rev)
                        .and_then(|rev| self.revision(rev));
                    accepted(handle, assigned, revision)
                }
            };

            results.push(result);
        }

        Ok(results)
    }
}

fn accepted(
    handle: ReplicaHandle,
    assigned: Option<ReplicaHandle>,
    revision: Option<String>,
) -> ReplicaPushResult {
    ReplicaPushResult {
        handle,
        outcome: ReplicaPushOutcome::Accepted,
        assigned,
        revision,
    }
}

fn rejected(handle: ReplicaHandle) -> ReplicaPushResult {
    ReplicaPushResult {
        handle,
        outcome: ReplicaPushOutcome::Rejected,
        assigned: None,
        revision: None,
    }
}
