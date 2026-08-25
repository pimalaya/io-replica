//! Reference: soft-delete retention at the storage seam.
//!
//! A remote delete reaches the consumer as a
//! [`ReplicaWriteOp::DropPlacement`]. A backup storage MAY treat that
//! drop as a soft-delete, retaining the row marked removed-upstream and
//! hiding it from `load`, rather than a hard delete. The merge only sees
//! what `load` returns, so hiding the row keeps the engine from
//! re-deriving against it and the retained copy survives every later
//! sync: the retention decision lives in the storage, with no merge-core
//! change.

// NOTE: shared across test targets; this one uses only the remote helpers
#[allow(dead_code)]
mod common;

use std::{
    collections::{BTreeMap, BTreeSet},
    convert::Infallible,
};

use io_replica::{
    change::ReplicaWriteOp,
    client::{ReplicaClient, ReplicaStorage},
    collection::{ReplicaCheckpoint, ReplicaCollectionId},
    object::{ReplicaHash, ReplicaObject},
    placement::{ReplicaHandle, ReplicaLinkId, ReplicaPlacement},
    storage::{ReplicaLoadScope, ReplicaLoaded},
    sync::{ReplicaSyncOptions, ReplicaSyncReport},
};

use crate::common::MemRemote;

/// A storage whose `DropPlacement` soft-deletes: the row is retained and
/// marked hidden, and `load` skips hidden rows.
#[derive(Default)]
struct SoftDeleteStorage {
    placements: BTreeMap<(ReplicaCollectionId, ReplicaHandle), ReplicaPlacement>,
    hidden: BTreeSet<(ReplicaCollectionId, ReplicaHandle)>,
    objects: BTreeMap<ReplicaHash, (ReplicaObject, Vec<u8>)>,
    checkpoints: BTreeMap<ReplicaCollectionId, ReplicaCheckpoint>,
}

impl SoftDeleteStorage {
    /// Whether `handle` is retained (present but hidden) after a soft-delete.
    fn is_retained(&self, collection: &str, handle: &str) -> bool {
        let key = (collection.into(), ReplicaHandle::from(handle));
        self.placements.contains_key(&key) && self.hidden.contains(&key)
    }
}

impl ReplicaStorage for SoftDeleteStorage {
    type Error = Infallible;

    fn load(
        &self,
        collection: &ReplicaCollectionId,
        _: &ReplicaLoadScope,
    ) -> Result<ReplicaLoaded, Infallible> {
        let placements = self
            .placements
            .iter()
            .filter(|((c, h), _)| c == collection && !self.hidden.contains(&(c.clone(), h.clone())))
            .map(|(_, p)| p.clone())
            .collect();
        Ok(ReplicaLoaded {
            placements,
            checkpoint: self.checkpoints.get(collection).cloned(),
        })
    }

    fn lookup_objects(
        &self,
        links: &[ReplicaLinkId],
    ) -> Result<BTreeMap<ReplicaLinkId, ReplicaHash>, Infallible> {
        let mut known = BTreeMap::new();
        for link in links {
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
                    let key = (p.collection.clone(), p.handle.clone());
                    self.hidden.remove(&key);
                    self.placements.insert(key, p);
                }
                ReplicaWriteOp::DropPlacement {
                    collection, handle, ..
                } => {
                    // NOTE: retain the row and hide it rather than remove
                    // it.
                    self.hidden.insert((collection, handle));
                }
                ReplicaWriteOp::StoreObject { object, body } => {
                    self.objects
                        .insert(object.hash.clone(), (object, body.unwrap_or_default()));
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

#[test]
fn soft_delete_retains_and_hides_a_remote_expunge() {
    let mut remote = MemRemote::default();
    remote.seed("inbox", "1", "m1", &[], b"body");
    let mut client = ReplicaClient::new(SoftDeleteStorage::default(), remote);

    client.sync("inbox", ReplicaSyncOptions::default()).unwrap();
    let loaded = client
        .storage()
        .load(&"inbox".into(), &ReplicaLoadScope::All)
        .unwrap();
    assert_eq!(loaded.placements.len(), 1, "the item is pulled");

    // the source expunges the item
    client.remote_mut().remove("inbox", "1");
    let report = client.sync("inbox", ReplicaSyncOptions::default()).unwrap();
    assert_eq!(report.pulled, 1, "the vanish is observed");

    // the row is retained for restore, but hidden from the offline view
    let loaded = client
        .storage()
        .load(&"inbox".into(), &ReplicaLoadScope::All)
        .unwrap();
    assert!(loaded.placements.is_empty(), "hidden from load");
    assert!(
        client.storage().is_retained("inbox", "1"),
        "the copy is kept for restore",
    );

    // a later sync must not re-derive against the hidden row
    let delta = client.sync("inbox", ReplicaSyncOptions::default()).unwrap();
    assert_eq!(delta, ReplicaSyncReport::default(), "quiescent delta sync");
    let full = client
        .sync(
            "inbox",
            ReplicaSyncOptions {
                full: true,
                ..Default::default()
            },
        )
        .unwrap();
    assert_eq!(full, ReplicaSyncReport::default(), "quiescent full sync");
    assert!(
        client.storage().is_retained("inbox", "1"),
        "still retained after re-sync",
    );
}
