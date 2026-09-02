//! A content conflict crossed with soft-delete retention.
//!
//! A backup storage answers a `DropPlacement` by retaining the row and
//! hiding it from `load` (tests/soft_delete.rs), so a copy survives an
//! upstream delete. A conflicted placement holds three bodies, and what
//! happens to that pair when the item's last binding vanishes is the
//! question a retaining consumer has to answer: whether the drop is
//! reached at all, what the retained row keeps, and whether a later sync
//! settles a divergence nobody decided.

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
    mutate::ReplicaMutation,
    object::{ReplicaHash, ReplicaObject},
    placement::{ReplicaHandle, ReplicaLinkId, ReplicaPlacement, ReplicaStatus},
    remote::ReplicaTier,
    storage::{ReplicaLoadScope, ReplicaLoaded},
    sync::{ReplicaSyncOptions, ReplicaSyncReport},
};

use crate::common::{MemRemote, hash};

const BASE: &[u8] = b"the ancestor body";
const LOCAL: &[u8] = b"the local body";
const REMOTE: &[u8] = b"the remote body";
const RETURNED: &[u8] = b"the body it came back with";

type Client = ReplicaClient<RetainingStorage, MemRemote>;

/// A storage whose `DropPlacement` soft-deletes: the row is retained and
/// marked hidden, and `load` skips hidden rows.
#[derive(Default)]
struct RetainingStorage {
    placements: BTreeMap<(ReplicaCollectionId, ReplicaHandle), ReplicaPlacement>,
    hidden: BTreeSet<(ReplicaCollectionId, ReplicaHandle)>,
    objects: BTreeMap<ReplicaHash, (ReplicaObject, Vec<u8>)>,
    checkpoints: BTreeMap<ReplicaCollectionId, ReplicaCheckpoint>,
}

impl RetainingStorage {
    /// The row retained under `handle`, present but hidden from `load`.
    fn retained(&self, collection: &str, handle: &str) -> Option<&ReplicaPlacement> {
        let key = (collection.into(), ReplicaHandle::from(handle));
        self.hidden
            .contains(&key)
            .then(|| self.placements.get(&key))?
    }

    /// The rows `load` still returns, in handle order: the offline view.
    fn live(&self, collection: &str) -> Vec<&ReplicaPlacement> {
        let collection = ReplicaCollectionId::from(collection);
        self.placements
            .iter()
            .filter(|((c, h), _)| {
                *c == collection && !self.hidden.contains(&(c.clone(), h.clone()))
            })
            .map(|(_, p)| p)
            .collect()
    }
}

impl ReplicaStorage for RetainingStorage {
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

/// A retaining client whose single inbox member is conflicted: the base
/// holds `BASE`, the placement `LOCAL`, and the remote `REMOTE` at the
/// recorded conflict revision, with the diverging body in the store.
fn conflicted_client() -> Client {
    let mut remote = MemRemote::default();
    remote.mutable = true;
    remote.seed("inbox", "m1", "l1", &[], BASE);

    let mut client = ReplicaClient::new(RetainingStorage::default(), remote);
    let opts = ReplicaSyncOptions::default();
    client.sync("inbox", opts).unwrap();
    client
        .upgrade("inbox", vec![ReplicaHandle::from("m1")], ReplicaTier::Full)
        .unwrap();

    edit(&mut client, LOCAL);
    client.remote_mut().edit("inbox", "m1", REMOTE);
    client.sync("inbox", opts).unwrap();
    client
        .upgrade("inbox", vec![ReplicaHandle::from("m1")], ReplicaTier::Full)
        .unwrap();

    let placement = client.storage().live("inbox");
    assert_eq!(placement.len(), 1);
    assert_eq!(placement[0].status, ReplicaStatus::Conflict);
    assert_eq!(placement[0].conflict_revision.as_deref(), Some("1"));
    assert_eq!(placement[0].conflict_object, Some(hash(REMOTE)));

    client
}

fn edit(client: &mut Client, body: &[u8]) {
    let object = ReplicaObject {
        hash: hash(body),
        size: body.len(),
    };
    client
        .mutate(
            "inbox",
            ReplicaMutation::Edit {
                handle: ReplicaHandle::from("m1"),
                object,
                body: body.to_vec(),
                meta: None,
                sort_key: None,
            },
        )
        .unwrap();
}

/// What the server holds for the inbox, as handle and body.
fn server(client: &Client) -> Vec<(String, Vec<u8>)> {
    client
        .remote()
        .items
        .get(&"inbox".into())
        .into_iter()
        .flatten()
        .map(|(handle, item)| (handle.as_str().to_string(), item.body.clone()))
        .collect()
}

/// Edit beats delete in both directions, and a conflict holds an edit:
/// the remote withdrawing its own side of the divergence makes the
/// divergence moot, so the item is resurrected as a pending create
/// rather than dropped, and retention never sees it.
#[test]
fn a_conflicted_item_the_remote_deletes_never_reaches_the_retained_row_path() {
    let mut client = conflicted_client();
    client.remote_mut().remove("inbox", "m1");

    let report = client.sync("inbox", ReplicaSyncOptions::default()).unwrap();

    assert_eq!(report.pushed, 1, "the local body is re-uploaded");
    assert_eq!(report.conflicts, 0, "the divergence is over, not re-run");
    assert_eq!(
        server(&client),
        vec![(
            "app-1".to_string(),
            hash(LOCAL).as_str().as_bytes().to_vec()
        )],
        "the local side of the divergence survives on the server",
    );

    let live = client.storage().live("inbox");
    assert_eq!(
        live.len(),
        1,
        "one live row, not a resurrection plus a copy"
    );
    assert_eq!(live[0].status, ReplicaStatus::Clean);
    assert_eq!(live[0].object, Some(hash(LOCAL)));
    assert_eq!(
        live[0].conflict_revision, None,
        "the conflict pair goes with the remote body it described",
    );
    assert_eq!(live[0].conflict_object, None);
}

/// The one way a conflicted row reaches the drop: the user asked for the
/// delete and the remote made the same call. The row is retained whole,
/// both sides of the divergence with it, so a restore hands back what
/// the user was deciding between rather than half of it, and no later
/// sync settles it for them.
#[test]
fn a_retained_conflict_keeps_both_bodies_and_is_never_settled_by_a_later_sync() {
    let mut client = conflicted_client();
    client
        .mutate("inbox", ReplicaMutation::Remove(ReplicaHandle::from("m1")))
        .unwrap();
    client.remote_mut().remove("inbox", "m1");

    client.sync("inbox", ReplicaSyncOptions::default()).unwrap();

    assert!(
        client.storage().live("inbox").is_empty(),
        "hidden from load"
    );
    let retained = client
        .storage()
        .retained("inbox", "m1")
        .expect("the row is kept for restore")
        .clone();
    assert_eq!(retained.status, ReplicaStatus::Tombstone);
    assert_eq!(retained.object, Some(hash(LOCAL)), "the local side");
    assert_eq!(retained.conflict_revision.as_deref(), Some("1"));
    assert_eq!(
        retained.conflict_object,
        Some(hash(REMOTE)),
        "and the remote side it was weighed against",
    );

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
    assert_eq!(
        client.storage().retained("inbox", "m1"),
        Some(&retained),
        "the retained divergence is untouched",
    );
}

/// The mirror case: the same logical item returns upstream under a fresh
/// handle while a retained row still holds an undecided divergence. The
/// arrival is a new row and adopts nothing, so the retained bodies stay
/// recoverable and the return settles nothing.
#[test]
fn an_item_coming_back_leaves_the_retained_conflict_alone() {
    let mut client = conflicted_client();
    client
        .mutate("inbox", ReplicaMutation::Remove(ReplicaHandle::from("m1")))
        .unwrap();
    client.remote_mut().remove("inbox", "m1");
    client.sync("inbox", ReplicaSyncOptions::default()).unwrap();

    // NOTE: restored upstream under the same link id and a fresh
    // handle, the shape a return takes on a server that never reuses
    // one.
    client.remote_mut().seed("inbox", "m2", "l1", &[], RETURNED);
    let report = client.sync("inbox", ReplicaSyncOptions::default()).unwrap();
    client
        .upgrade("inbox", vec![ReplicaHandle::from("m2")], ReplicaTier::Full)
        .unwrap();

    assert_eq!(report.pulled, 1, "a plain arrival");
    assert_eq!(report.conflicts, 0);
    assert_eq!(report.pushed, 0, "the retained row pushes nothing");

    let live = client.storage().live("inbox");
    assert_eq!(live.len(), 1);
    assert_eq!(live[0].handle, ReplicaHandle::from("m2"));
    assert_eq!(live[0].status, ReplicaStatus::Clean);
    assert_eq!(
        live[0].object,
        Some(hash(RETURNED)),
        "the arrival carries what the remote holds, not the retained body",
    );
    assert_eq!(live[0].conflict_revision, None);

    let retained = client
        .storage()
        .retained("inbox", "m1")
        .expect("still kept for restore");
    assert_eq!(retained.object, Some(hash(LOCAL)));
    assert_eq!(retained.conflict_object, Some(hash(REMOTE)));
}
