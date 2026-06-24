//! End-to-end concept validation with an in-memory storage and a fake
//! remote.
//!
//! Exercises the whole lifecycle on a generic collection of items: initial
//! pull, fully offline open, progressive upgrade with cross-collection
//! object dedup (one item present in two collections), local mutation,
//! push, remote pull, and a divergent-flags conflict. The in-memory
//! backends prove the engine drives the protocol purely through its
//! `Wants`, knowing nothing of either side.

use std::{collections::BTreeMap, convert::Infallible};

use io_offline::{
    change::{Change, WriteOp},
    client::{OfflineClient, Remote, Storage},
    collection::{Checkpoint, CollectionId},
    mutate::Mutation,
    object::{Hash, Object},
    placement::{Flags, Handle, Level, LinkId, Meta, Placement, Status},
    remote::{FetchedItem, PushOutcome, PushResult, RemoteItem, RemoteSnapshot, Tier},
    storage::Loaded,
    sync::OfflineSyncOptions,
};

// ---- in-memory storage ------------------------------------------------

#[derive(Default)]
struct MemStorage {
    placements: BTreeMap<(CollectionId, Handle), Placement>,
    objects: BTreeMap<Hash, (Object, Vec<u8>)>,
    checkpoints: BTreeMap<CollectionId, Checkpoint>,
}

impl MemStorage {
    fn placement(&self, collection: &str, handle: &str) -> &Placement {
        self.placements
            .get(&(collection.into(), Handle::from(handle)))
            .expect("placement exists")
    }
}

impl Storage for MemStorage {
    type Error = Infallible;

    fn load(&self, collection: &CollectionId) -> Result<Loaded, Infallible> {
        let placements = self
            .placements
            .iter()
            .filter(|((c, _), _)| c == collection)
            .map(|(_, p)| p.clone())
            .collect();
        let checkpoint = self.checkpoints.get(collection).cloned();

        Ok(Loaded {
            placements,
            checkpoint,
        })
    }

    fn lookup_objects(&self, links: &[LinkId]) -> Result<BTreeMap<LinkId, Hash>, Infallible> {
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

    fn write(&mut self, ops: Vec<WriteOp>) -> Result<(), Infallible> {
        for op in ops {
            match op {
                WriteOp::UpsertPlacement(p) => {
                    self.placements
                        .insert((p.collection.clone(), p.handle.clone()), p);
                }
                WriteOp::DropPlacement { collection, handle } => {
                    self.placements.remove(&(collection, handle));
                }
                WriteOp::StoreObject { object, body } => {
                    self.objects.insert(object.hash.clone(), (object, body));
                }
                WriteOp::SetBase {
                    collection,
                    handle,
                    base,
                } => {
                    if let Some(p) = self.placements.get_mut(&(collection, handle)) {
                        p.base = Some(base);
                    }
                }
                WriteOp::SetCheckpoint {
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
struct ServerItem {
    link_id: LinkId,
    flags: Flags,
    body: Vec<u8>,
}

#[derive(Default)]
struct MemRemote {
    items: BTreeMap<CollectionId, BTreeMap<Handle, ServerItem>>,
    full_fetches: Vec<Handle>,
    calls: usize,
}

impl MemRemote {
    fn seed(&mut self, collection: &str, handle: &str, link: &str, flags: &[&str], body: &[u8]) {
        self.items.entry(collection.into()).or_default().insert(
            Handle::from(handle),
            ServerItem {
                link_id: LinkId::from(link),
                flags: Flags::from_iter(flags.iter().copied()),
                body: body.to_vec(),
            },
        );
    }

    fn set_flags(&mut self, collection: &str, handle: &str, flags: &[&str]) {
        self.items
            .get_mut(&collection.into())
            .and_then(|c| c.get_mut(&Handle::from(handle)))
            .expect("server item exists")
            .flags = Flags::from_iter(flags.iter().copied());
    }

    fn flags_of(&self, collection: &str, handle: &str) -> &Flags {
        &self
            .items
            .get(&collection.into())
            .and_then(|c| c.get(&Handle::from(handle)))
            .expect("server item exists")
            .flags
    }
}

fn hash(body: &[u8]) -> Hash {
    // tiny deterministic content hash: identical bytes collapse to one
    // object, which is exactly what the dedup path keys on
    let mut acc: u64 = 1469598103934665603;
    for byte in body {
        acc ^= *byte as u64;
        acc = acc.wrapping_mul(1099511628211);
    }
    Hash::from(format!("{acc:016x}"))
}

impl Remote for MemRemote {
    type Error = Infallible;

    fn count(&mut self, collection: &CollectionId) -> Result<usize, Infallible> {
        self.calls += 1;
        Ok(self.items.get(collection).map(|c| c.len()).unwrap_or(0))
    }

    fn enumerate(
        &mut self,
        collection: &CollectionId,
        _cursor: Option<Checkpoint>,
    ) -> Result<RemoteSnapshot, Infallible> {
        self.calls += 1;

        let items = self
            .items
            .get(collection)
            .into_iter()
            .flatten()
            .map(|(handle, item)| RemoteItem {
                handle: handle.clone(),
                flags: item.flags.clone(),
            })
            .collect();

        Ok(RemoteSnapshot {
            items,
            vanished: Vec::new(),
            complete: true,
            checkpoint: Checkpoint(b"server-state".to_vec()),
        })
    }

    fn fetch(
        &mut self,
        collection: &CollectionId,
        handles: Vec<Handle>,
        tier: Tier,
    ) -> Result<Vec<FetchedItem>, Infallible> {
        self.calls += 1;

        let collection = self.items.get(collection).cloned().unwrap_or_default();
        let mut out = Vec::new();

        for handle in handles {
            let item = collection.get(&handle).expect("fetched handle exists");
            let meta = Meta(format!("headers:{}", handle.as_str()));

            let body = match tier {
                Tier::Meta => None,
                Tier::Full => {
                    self.full_fetches.push(handle.clone());
                    Some((hash(&item.body), item.body.clone()))
                }
            };

            out.push(FetchedItem {
                handle,
                link_id: item.link_id.clone(),
                meta,
                body,
            });
        }

        Ok(out)
    }

    fn push(
        &mut self,
        collection: &CollectionId,
        changes: Vec<Change>,
    ) -> Result<Vec<PushResult>, Infallible> {
        self.calls += 1;
        let server = self.items.entry(collection.clone()).or_default();
        let mut results = Vec::new();

        for change in changes {
            let handle = match change {
                Change::SetFlags { handle, flags } => {
                    if let Some(item) = server.get_mut(&handle) {
                        item.flags = flags;
                    }
                    handle
                }
                Change::Remove(handle) => {
                    server.remove(&handle);
                    handle
                }
                Change::Add { handle, .. } => handle,
            };

            results.push(PushResult {
                handle,
                outcome: PushOutcome::Accepted,
            });
        }

        Ok(results)
    }
}

// ---- the scenario -----------------------------------------------------

fn seeded_client() -> OfflineClient<MemStorage, MemRemote> {
    let body_a = b"From: a\r\nMessage-ID: <msg-a>\r\n\r\nshared body\r\n";
    let body_b = b"From: b\r\nMessage-ID: <msg-b>\r\n\r\nother body\r\n";

    let mut remote = MemRemote::default();
    // NOTE: inbox/i1 and archive/a1 are the SAME logical item (link msg-a,
    // identical body) present in two collections: the dedup case
    remote.seed("inbox", "i1", "msg-a", &[], body_a);
    remote.seed("inbox", "i2", "msg-b", &[], body_b);
    remote.seed("archive", "a1", "msg-a", &["seen"], body_a);

    OfflineClient::new(MemStorage::default(), remote)
}

#[test]
fn full_offline_lifecycle() {
    let mut client = seeded_client();

    // 1. initial sync pulls a complete probed spine
    let report = client.sync("inbox", OfflineSyncOptions::default()).unwrap();
    assert_eq!(report.pulled, 2, "both inbox items pulled");
    assert_eq!(report.pushed, 0);

    // 2. open is fully offline: it must touch storage only, never the
    // remote
    let calls_before = client.remote().calls;
    let loaded = client.open("inbox").unwrap();
    assert_eq!(loaded.placements.len(), 2);
    assert!(loaded.placements.iter().all(|p| p.level == Level::Probed));
    assert_eq!(
        client.remote().calls,
        calls_before,
        "open must not hit the remote",
    );

    // 3. progressive upgrade: headers, then one body
    let report = client
        .upgrade(
            "inbox",
            vec![Handle::from("i1"), Handle::from("i2")],
            Tier::Meta,
        )
        .unwrap();
    assert_eq!(report.upgraded, 2);
    assert_eq!(client.storage().placement("inbox", "i1").level, Level::Meta);

    let report = client
        .upgrade("inbox", vec![Handle::from("i1")], Tier::Full)
        .unwrap();
    assert_eq!(report.fetched, 1);
    assert_eq!(report.deduped, 0);
    assert_eq!(client.storage().placement("inbox", "i1").level, Level::Full);
    assert_eq!(client.storage().objects.len(), 1, "one stored body");

    // 4. second collection, same logical item: upgrading its body must
    // dedup against the already-stored object, with zero new fetch.
    // Meta first resolves a1's link id (enumerate does not carry it), then
    // the Full upgrade links the shared body by that link id.
    client
        .sync("archive", OfflineSyncOptions::default())
        .unwrap();
    client
        .upgrade("archive", vec![Handle::from("a1")], Tier::Meta)
        .unwrap();
    let fetches_before = client.remote().full_fetches.len();

    let report = client
        .upgrade("archive", vec![Handle::from("a1")], Tier::Full)
        .unwrap();
    assert_eq!(report.deduped, 1, "shared body deduped");
    assert_eq!(report.fetched, 0);
    assert_eq!(
        client.remote().full_fetches.len(),
        fetches_before,
        "dedup must skip the network fetch",
    );
    assert_eq!(client.storage().objects.len(), 1, "still one stored body");
    // the two placements share one object but keep distinct flags
    assert_eq!(
        client.storage().placement("inbox", "i1").object,
        client.storage().placement("archive", "a1").object,
    );

    // 5. local mutation is offline, then pushed on sync
    client
        .mutate(
            "inbox",
            Mutation::SetFlags {
                handle: Handle::from("i1"),
                flags: Flags::from_iter(["seen"]),
            },
        )
        .unwrap();
    assert_eq!(
        client.storage().placement("inbox", "i1").status,
        Status::Dirty,
    );

    let report = client.sync("inbox", OfflineSyncOptions::default()).unwrap();
    assert_eq!(report.pushed, 1, "local seen flag pushed");
    assert!(client.remote().flags_of("inbox", "i1").contains("seen"));
    assert_eq!(
        client.storage().placement("inbox", "i1").status,
        Status::Clean,
        "pushed placement is rebased clean",
    );

    // 6. a remote-side change is pulled on the next sync
    client.remote_mut().set_flags("inbox", "i2", &["flagged"]);
    let report = client.sync("inbox", OfflineSyncOptions::default()).unwrap();
    assert_eq!(report.pulled, 1);
    assert!(
        client
            .storage()
            .placement("inbox", "i2")
            .flags
            .contains("flagged")
    );

    // 7. divergent edits on both sides keep both: a conflict, never a
    // silent loss
    client
        .mutate(
            "inbox",
            Mutation::SetFlags {
                handle: Handle::from("i1"),
                flags: Flags::from_iter(["draft"]),
            },
        )
        .unwrap();
    client
        .remote_mut()
        .set_flags("inbox", "i1", &["seen", "important"]);

    let report = client.sync("inbox", OfflineSyncOptions::default()).unwrap();
    assert_eq!(report.conflicts, 1);
    assert_eq!(
        client.storage().placement("inbox", "i1").status,
        Status::Conflict,
    );
}
