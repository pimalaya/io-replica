//! A complete offline lifecycle over an in-memory storage and a fake
//! IMAP-like server, printing what happens at every step.
//!
//! The story: two mailboxes, inbox and archive, where one message was
//! server-side copied into archive. The example probes the inbox spine,
//! opens it fully offline, hydrates summaries then one body, dedups the
//! archive copy against the object store (no second download), flags a
//! message offline, pushes the flag on the next sync, and finally pulls
//! a flag another client set remotely. An address book or a calendar
//! follows the exact same shape; only the seam impls change.
//!
//! Run it with: cargo run --example mailbox_lifecycle

use std::{collections::BTreeMap, convert::Infallible};

use io_replica::{
    change::{ReplicaChange, ReplicaWriteOp},
    client::{ReplicaClient, ReplicaRemote, ReplicaStorage},
    collection::{ReplicaCheckpoint, ReplicaCollectionId},
    mutate::ReplicaMutation,
    object::ReplicaHash,
    placement::{ReplicaFlags, ReplicaHandle, ReplicaLinkId, ReplicaMeta},
    remote::{
        ReplicaFetchedBody, ReplicaFetchedItem, ReplicaPushOutcome, ReplicaPushResult,
        ReplicaRemoteItem, ReplicaRemoteSnapshot, ReplicaTier,
    },
    storage::ReplicaLoaded,
    sync::ReplicaSyncOptions,
};

/// The local index plus blob store: two maps and a checkpoint per
/// collection. A real consumer backs this with sqlite plus a blob dir.
#[derive(Default)]
struct MemStorage {
    placements:
        BTreeMap<(ReplicaCollectionId, ReplicaHandle), io_replica::placement::ReplicaPlacement>,
    objects: BTreeMap<ReplicaHash, Vec<u8>>,
    checkpoints: BTreeMap<ReplicaCollectionId, ReplicaCheckpoint>,
}

impl ReplicaStorage for MemStorage {
    type Error = Infallible;

    fn load(&self, collection: &ReplicaCollectionId) -> Result<ReplicaLoaded, Infallible> {
        Ok(ReplicaLoaded {
            placements: self
                .placements
                .iter()
                .filter(|((c, _), _)| c == collection)
                .map(|(_, p)| p.clone())
                .collect(),
            checkpoint: self.checkpoints.get(collection).cloned(),
        })
    }

    fn lookup_objects(
        &self,
        links: &[ReplicaLinkId],
    ) -> Result<BTreeMap<ReplicaLinkId, ReplicaHash>, Infallible> {
        // NOTE: the dedup seam: any placement, in any collection, that
        // already links one of these logical items to a stored body.
        let mut known = BTreeMap::new();
        for link in links {
            let hit = self.placements.values().find_map(|p| {
                (p.link_id.as_ref() == Some(link))
                    .then(|| p.object.clone())
                    .flatten()
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
                    self.objects.insert(object.hash, body.unwrap_or_default());
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

/// One message on the fake server.
struct ServerItem {
    link: ReplicaLinkId,
    flags: ReplicaFlags,
    body: Vec<u8>,
}

/// An IMAP-like server: immutable message content (no revisions),
/// complete snapshots on every enumerate. A real consumer drives a
/// protocol crate (io-imap, io-jmap, io-webdav) here instead.
#[derive(Default)]
struct MemRemote {
    collections: BTreeMap<ReplicaCollectionId, BTreeMap<ReplicaHandle, ServerItem>>,
    body_fetches: usize,
}

impl MemRemote {
    fn seed(&mut self, collection: &str, handle: &str, link: &str, subject: &str, body: &[u8]) {
        self.collections
            .entry(collection.into())
            .or_default()
            .insert(
                ReplicaHandle::from(handle),
                ServerItem {
                    link: ReplicaLinkId::from(link),
                    flags: ReplicaFlags::default(),
                    body: format!(
                        "Subject: {subject}\r\n\r\n{}",
                        String::from_utf8_lossy(body)
                    )
                    .into_bytes(),
                },
            );
    }
}

impl ReplicaRemote for MemRemote {
    type Error = Infallible;

    fn enumerate(
        &mut self,
        collection: &ReplicaCollectionId,
        _cursor: Option<ReplicaCheckpoint>,
    ) -> Result<ReplicaRemoteSnapshot, Infallible> {
        Ok(ReplicaRemoteSnapshot {
            items: self
                .collections
                .get(collection)
                .into_iter()
                .flatten()
                .map(|(handle, item)| ReplicaRemoteItem {
                    handle: handle.clone(),
                    flags: item.flags.clone(),
                    revision: None,
                })
                .collect(),
            vanished: Vec::new(),
            complete: true,
            checkpoint: ReplicaCheckpoint(b"cp".to_vec()),
        })
    }

    fn fetch(
        &mut self,
        collection: &ReplicaCollectionId,
        handles: Vec<ReplicaHandle>,
        tier: ReplicaTier,
    ) -> Result<Vec<ReplicaFetchedItem>, Infallible> {
        let members = &self.collections[collection];
        let mut out = Vec::new();
        for handle in handles {
            let item = &members[&handle];
            let body = match tier {
                ReplicaTier::Meta => None,
                ReplicaTier::Full => {
                    self.body_fetches += 1;
                    Some(ReplicaFetchedBody::Inline {
                        hash: hash(&item.body),
                        bytes: item.body.clone(),
                    })
                }
            };
            out.push(ReplicaFetchedItem {
                sort_key: Default::default(),
                handle,
                link_id: item.link.clone(),
                meta: ReplicaMeta(
                    String::from_utf8_lossy(&item.body)
                        .lines()
                        .next()
                        .unwrap()
                        .into(),
                ),
                body,
                revision: None,
            });
        }
        Ok(out)
    }

    fn push(
        &mut self,
        collection: &ReplicaCollectionId,
        changes: Vec<ReplicaChange>,
    ) -> Result<Vec<ReplicaPushResult>, Infallible> {
        let mut results = Vec::new();
        for change in changes {
            let ReplicaChange::SetFlags { handle, flags } = change else {
                unreachable!("this example only stages flag changes");
            };
            let members = self.collections.get_mut(collection).unwrap();
            members.get_mut(&handle).unwrap().flags = flags;
            results.push(ReplicaPushResult {
                handle,
                outcome: ReplicaPushOutcome::Accepted,
                assigned: None,
                revision: None,
            });
        }
        Ok(results)
    }
}

/// A tiny deterministic content hash: identical bytes collapse to one
/// object, which is exactly what the dedup path keys on.
fn hash(body: &[u8]) -> ReplicaHash {
    let mut acc: u64 = 1469598103934665603;
    for byte in body {
        acc ^= *byte as u64;
        acc = acc.wrapping_mul(1099511628211);
    }
    ReplicaHash::from(format!("{acc:016x}"))
}

fn main() {
    // The server holds two messages in inbox; msg-1 was also server-side
    // copied into archive, so both placements share one logical item.
    let mut remote = MemRemote::default();
    remote.seed("inbox", "1", "msg-1", "Lunch tomorrow?", b"Nope, Thursday!");
    remote.seed("inbox", "2", "msg-2", "Invoice #42", b"Attached.");
    remote.seed(
        "archive",
        "9",
        "msg-1",
        "Lunch tomorrow?",
        b"Nope, Thursday!",
    );

    let mut client = ReplicaClient::new(MemStorage::default(), remote);

    // 1. First sync probes the spine: every handle and flag set, no body.
    let report = client.sync("inbox", ReplicaSyncOptions::default()).unwrap();
    println!(
        "1. synced inbox: {} placements pulled, no body fetched",
        report.pulled
    );

    // 2. Open is fully offline: it reads storage, never the network.
    let loaded = client.open("inbox").unwrap();
    println!("2. opened inbox offline: {} rows", loaded.placements.len());

    // 3. Hydrate summaries (a list screen), then one body (a detail screen).
    let handles = vec![ReplicaHandle::from("1"), ReplicaHandle::from("2")];
    client.upgrade("inbox", handles, ReplicaTier::Meta).unwrap();
    let report = client
        .upgrade("inbox", vec![ReplicaHandle::from("1")], ReplicaTier::Full)
        .unwrap();
    println!(
        "3. upgraded: {} body fetched from the remote",
        report.fetched
    );

    // 4. The archive copy dedups: its link id already maps to a stored
    //    object, so the body is linked without any download.
    client
        .sync("archive", ReplicaSyncOptions::default())
        .unwrap();
    client
        .upgrade("archive", vec![ReplicaHandle::from("9")], ReplicaTier::Meta)
        .unwrap();
    let report = client
        .upgrade("archive", vec![ReplicaHandle::from("9")], ReplicaTier::Full)
        .unwrap();
    println!(
        "4. archive copy: {} deduped, {} fetched ({} object stored once, {} body downloads total)",
        report.deduped,
        report.fetched,
        client.storage().objects.len(),
        client.remote().body_fetches,
    );

    // 5. Flag the message offline: storage only, marked dirty for later.
    let mutation = ReplicaMutation::SetFlags {
        handle: ReplicaHandle::from("1"),
        flags: ReplicaFlags::from_iter(["seen"]),
    };
    client.mutate("inbox", mutation).unwrap();
    println!("5. flagged inbox/1 as seen, offline");

    // 6. The next sync derives the pending push and the server confirms it.
    let report = client.sync("inbox", ReplicaSyncOptions::default()).unwrap();
    let server_flags =
        &client.remote().collections[&"inbox".into()][&ReplicaHandle::from("1")].flags;
    println!(
        "6. synced: {} pushed, server now sees {:?}",
        report.pushed,
        server_flags.known()
    );

    // 7. Another client flags msg-2 remotely; the next sync pulls it.
    let members = client
        .remote_mut()
        .collections
        .get_mut(&"inbox".into())
        .unwrap();
    members.get_mut(&ReplicaHandle::from("2")).unwrap().flags =
        ReplicaFlags::from_iter(["flagged"]);
    let report = client.sync("inbox", ReplicaSyncOptions::default()).unwrap();
    let local = client.storage().placements[&("inbox".into(), ReplicaHandle::from("2"))].clone();
    println!(
        "7. synced: {} pulled, inbox/2 locally carries {:?}",
        report.pulled,
        local.flags.known()
    );
}
