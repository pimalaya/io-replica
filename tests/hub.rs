//! The hub driven by the real engine, which its own unit tests cannot do.
//!
//! `ReplicaHub::project` and `absorb` are covered in the crate over
//! hand-built writes, so what has never run is the loop they exist for:
//! project a source's view, let a real sync merge and push it, absorb
//! what it wrote, project again. Convergence lives in that loop, not in
//! either half.
//!
//! One `HubStore` stands for the shared storage; each source gets a
//! `SourceStore` view of it and its own remote, which is how a
//! multi-source consumer wires it.

// NOTE: shared across test targets; not every target uses every helper
#[allow(dead_code)]
mod common;

use std::{cell::RefCell, collections::BTreeMap, convert::Infallible, rc::Rc};

use io_replica::{
    change::ReplicaWriteOp,
    client::{ReplicaClient, ReplicaStorage},
    collection::{ReplicaCheckpoint, ReplicaCollectionId},
    hub::{ReplicaHub, ReplicaSourceId},
    object::{ReplicaHash, ReplicaObject},
    placement::{ReplicaHandle, ReplicaLinkId, ReplicaPlacement, ReplicaStatus},
    remote::ReplicaTier,
    storage::{ReplicaLoadScope, ReplicaLoaded},
    sync::{ReplicaDeletePolicy, ReplicaPushRights, ReplicaSyncOptions},
};

use crate::common::{MemRemote, MemStorage};

/// The shared store behind every source: one hub, one object store, and a
/// checkpoint per source.
///
/// The hub keys items by link id and `absorb` drops a placement with
/// none, which every row a sync pulls is: an enumeration yields handles,
/// and the link id lands on the first meta fetch. A hub-backed store has
/// to hold those rows itself until they are hubbed, which is what
/// `residual` and io-pimdir's residual list are.
#[derive(Default)]
struct HubStore {
    hub: ReplicaHub,
    /// Rows no link id has hubbed yet, keyed per source.
    residual: BTreeMap<(ReplicaSourceId, ReplicaHandle), ReplicaPlacement>,
    objects: BTreeMap<ReplicaHash, (ReplicaObject, Vec<u8>)>,
    checkpoints: BTreeMap<(ReplicaSourceId, ReplicaCollectionId), ReplicaCheckpoint>,
}

/// One source's view of the shared store, which is what the engine syncs
/// against: a load projects the hub for this source, a write absorbs
/// back into it.
struct SourceStore {
    source: ReplicaSourceId,
    shared: Rc<RefCell<HubStore>>,
}

impl ReplicaStorage for SourceStore {
    type Error = Infallible;

    fn load(
        &self,
        collection: &ReplicaCollectionId,
        _scope: &ReplicaLoadScope,
    ) -> Result<ReplicaLoaded, Infallible> {
        let shared = self.shared.borrow();
        let mut placements = shared.hub.project(collection, &self.source);
        placements.extend(
            shared
                .residual
                .iter()
                .filter(|((source, _), p)| source == &self.source && &p.collection == collection)
                .map(|(_, p)| p.clone()),
        );
        let checkpoint = shared
            .checkpoints
            .get(&(self.source.clone(), collection.clone()))
            .cloned();

        Ok(ReplicaLoaded {
            placements,
            checkpoint,
        })
    }

    fn lookup_objects(
        &self,
        links: &[ReplicaLinkId],
    ) -> Result<BTreeMap<ReplicaLinkId, ReplicaHash>, Infallible> {
        let shared = self.shared.borrow();
        let known = links
            .iter()
            .filter_map(|link| {
                let item = shared.hub.items.get(link)?;
                Some((link.clone(), item.object.clone()?))
            })
            .collect();

        Ok(known)
    }

    fn write(&mut self, ops: Vec<ReplicaWriteOp>) -> Result<(), Infallible> {
        let mut shared = self.shared.borrow_mut();

        for op in &ops {
            match op {
                ReplicaWriteOp::StoreObject { object, body } => {
                    shared.objects.insert(
                        object.hash.clone(),
                        (object.clone(), body.clone().unwrap_or_default()),
                    );
                }
                ReplicaWriteOp::SetCheckpoint {
                    collection,
                    checkpoint,
                } => {
                    let key = (self.source.clone(), collection.clone());
                    shared.checkpoints.insert(key, checkpoint.clone());
                }
                // NOTE: a row the hub cannot key is held here until a
                // link id hubs it; one it can is the hub's.
                ReplicaWriteOp::UpsertPlacement(placement) => {
                    let key = (self.source.clone(), placement.handle.clone());
                    match placement.link_id.is_some() {
                        true => shared.residual.remove(&key),
                        false => shared.residual.insert(key, placement.clone()),
                    };
                }
                ReplicaWriteOp::DropPlacement { handle, .. } => {
                    let key = (self.source.clone(), handle.clone());
                    shared.residual.remove(&key);
                }
            }
        }

        let source = self.source.clone();
        shared.hub.absorb(&source, &ops);
        Ok(())
    }
}

/// Two sources over one hub, each with its own server.
struct Mirror {
    shared: Rc<RefCell<HubStore>>,
    a: ReplicaClient<SourceStore, MemRemote>,
    b: ReplicaClient<SourceStore, MemRemote>,
}

impl Mirror {
    fn new() -> Self {
        let shared = Rc::new(RefCell::new(HubStore::default()));
        let source = |name: &str| SourceStore {
            source: ReplicaSourceId::from(name),
            shared: Rc::clone(&shared),
        };

        Self {
            a: ReplicaClient::new(source("a"), MemRemote::default()),
            b: ReplicaClient::new(source("b"), MemRemote::default()),
            shared,
        }
    }

    /// Syncs and hydrates both sources until the hub stops changing,
    /// which is what a mirroring consumer's scheduler does.
    ///
    /// Hydration is not optional: a pulled row carries no link id, so the
    /// hub cannot key it, and a body the hub does not hold is a member it
    /// cannot offer another source. A mirror is a sync plus an upgrade.
    fn quiesce(&mut self, opts: ReplicaSyncOptions) {
        for round in 0..8 {
            let before = self.shared.borrow().hub.clone();
            self.round(opts);
            if self.shared.borrow().hub == before {
                return;
            }
            assert!(round < 7, "the hub never settled");
        }
    }

    /// One pass over both sources: sync, then hydrate every row it left.
    fn round(&mut self, opts: ReplicaSyncOptions) {
        self.round_with(opts, opts);
    }

    /// A pass where the two sources are tuned differently, which is the
    /// point of a hub: one may push what the other refuses.
    fn round_with(&mut self, a: ReplicaSyncOptions, b: ReplicaSyncOptions) {
        for (source, opts) in [('a', a), ('b', b)] {
            let client = match source {
                'a' => &mut self.a,
                _ => &mut self.b,
            };
            client.sync("inbox", opts).unwrap();

            let handles: Vec<ReplicaHandle> = client
                .open("inbox")
                .unwrap()
                .placements
                .iter()
                .map(|p| p.handle.clone())
                .collect();
            client.upgrade("inbox", handles, ReplicaTier::Full).unwrap();
        }
    }

    /// Whether the hub knows the item was deleted on some source.
    fn deleted(&self, link: &str) -> Option<bool> {
        let shared = self.shared.borrow();
        let item = shared.hub.items.get(&ReplicaLinkId::from(link))?;
        Some(item.deleted)
    }

    /// The handles a source's server holds, in order.
    fn server(&self, source: char) -> Vec<String> {
        let remote = match source {
            'a' => self.a.remote(),
            _ => self.b.remote(),
        };
        remote
            .items
            .get(&"inbox".into())
            .map(|c| c.keys().map(|h| h.as_str().to_string()).collect())
            .unwrap_or_default()
    }

    /// Every link the hub holds, with the sources bound to it.
    fn bindings(&self) -> Vec<(String, Vec<String>)> {
        self.shared
            .borrow()
            .hub
            .items
            .iter()
            .map(|(link, item)| {
                let sources = item
                    .sources
                    .keys()
                    .map(|s| s.as_str().to_string())
                    .collect();
                (link.as_str().to_string(), sources)
            })
            .collect()
    }
}

/// The loop the hub exists for: a member only one source holds is offered
/// to the other, appended there, and absorbed back as one shared item.
#[test]
fn a_member_one_source_holds_is_mirrored_to_the_other() {
    let mut mirror = Mirror::new();
    mirror
        .a
        .remote_mut()
        .seed("inbox", "a1", "msg-a", &[], b"body a");

    mirror.quiesce(ReplicaSyncOptions::default());

    assert_eq!(mirror.server('a'), ["a1"], "the source keeps its member");
    assert_eq!(
        mirror.server('b').len(),
        1,
        "the member is appended to the other source: {:?}",
        mirror.server('b'),
    );
    assert_eq!(
        mirror.bindings(),
        [("msg-a".to_string(), vec!["a".to_string(), "b".to_string()])],
        "one shared item, bound to both sources",
    );
}

/// A flag set on one source reaches the other through the hub, and settles.
#[test]
fn a_flag_change_propagates_across_sources() {
    let mut mirror = Mirror::new();
    mirror
        .a
        .remote_mut()
        .seed("inbox", "a1", "msg-a", &[], b"body a");
    mirror.quiesce(ReplicaSyncOptions::default());

    let handle = mirror.server('b')[0].clone();
    mirror.a.remote_mut().set_flags("inbox", "a1", &["seen"]);
    mirror.quiesce(ReplicaSyncOptions::default());

    assert!(
        mirror
            .b
            .remote()
            .flags_of("inbox", &handle)
            .contains("seen"),
        "the flag reached the other source's server",
    );
}

/// A delete on one source propagates to the other, which is the rule
/// `ReplicaDropReason` exists to gate.
#[test]
fn a_delete_propagates_across_sources() {
    let mut mirror = Mirror::new();
    mirror
        .a
        .remote_mut()
        .seed("inbox", "a1", "msg-a", &[], b"body a");
    mirror.quiesce(ReplicaSyncOptions::default());
    assert_eq!(mirror.server('b').len(), 1);

    mirror.a.remote_mut().remove("inbox", "a1");
    mirror.quiesce(ReplicaSyncOptions::default());

    assert!(mirror.server('b').is_empty(), "the delete reached b");
    assert!(
        mirror.shared.borrow().hub.items.is_empty(),
        "no source holds it, so the hub drops it",
    );
}

/// A source refusing removes holds its copy, and under `Keep` that is all
/// it does: the deletion stands for every source that took it.
#[test]
fn a_source_refusing_removes_holds_its_copy_under_keep() {
    let mut mirror = Mirror::new();
    mirror
        .a
        .remote_mut()
        .seed("inbox", "a1", "msg-a", &[], b"body a");
    mirror.quiesce(ReplicaSyncOptions::default());
    let held = mirror.server('b')[0].clone();

    // b takes appends and flag changes, but never a delete
    let no_removes = ReplicaSyncOptions {
        rights: ReplicaPushRights {
            remove: false,
            ..ReplicaPushRights::all()
        },
        delete: ReplicaDeletePolicy::Keep,
        ..Default::default()
    };
    mirror.a.remote_mut().remove("inbox", "a1");
    for _ in 0..3 {
        mirror.round_with(ReplicaSyncOptions::default(), no_removes);
    }

    assert_eq!(
        mirror.server('b'),
        [held],
        "b's server still holds the member it refuses to delete",
    );
    assert_eq!(
        mirror.deleted("msg-a"),
        Some(true),
        "the hub keeps the deletion, so no source is offered it back",
    );
    assert!(
        mirror.server('a').is_empty(),
        "and the source that deleted it does not get it back: {:?}",
        mirror.server('a'),
    );
}

/// The same scenario under the default `Revert` policy, which is not the
/// same answer: b's revert reads as add-beats-delete across sources, so
/// the item is alive again and the hub mirrors it back to the source that
/// deleted it.
///
/// A hub-bound source wants `Keep`. This is pinned rather than fixed
/// because both readings are coherent: reverting says the source still
/// holds it, and through a hub that is a statement about the item.
#[test]
fn a_reverted_delete_resurrects_the_item_across_the_hub() {
    let mut mirror = Mirror::new();
    mirror
        .a
        .remote_mut()
        .seed("inbox", "a1", "msg-a", &[], b"body a");
    mirror.quiesce(ReplicaSyncOptions::default());

    let no_removes = ReplicaSyncOptions {
        rights: ReplicaPushRights {
            remove: false,
            ..ReplicaPushRights::all()
        },
        ..Default::default()
    };
    mirror.a.remote_mut().remove("inbox", "a1");
    for _ in 0..3 {
        mirror.round_with(ReplicaSyncOptions::default(), no_removes);
    }

    assert_eq!(
        mirror.deleted("msg-a"),
        Some(false),
        "the revert cleared the deletion for every source",
    );
    assert_eq!(
        mirror.server('a').len(),
        1,
        "so the item comes back to the source it was deleted on",
    );
}

/// A source that only pulls is mirrored into, never out of: the hub offers
/// it nothing it would have to push.
#[test]
fn a_read_only_source_receives_nothing_it_cannot_push() {
    let mut mirror = Mirror::new();
    mirror
        .a
        .remote_mut()
        .seed("inbox", "a1", "msg-a", &[], b"body a");

    let read_only = ReplicaSyncOptions {
        push: false,
        ..Default::default()
    };
    for _ in 0..3 {
        mirror.round_with(ReplicaSyncOptions::default(), read_only);
    }

    assert!(
        mirror.server('b').is_empty(),
        "a read-only source is never appended to: {:?}",
        mirror.server('b'),
    );
    let shared = mirror.shared.borrow();
    let item = shared.hub.items.get(&ReplicaLinkId::from("msg-a")).unwrap();
    let binding = item.sources.get(&ReplicaSourceId::from("b"));
    assert!(
        binding.is_none_or(|b| b.base.is_none()),
        "b never synced it, so it holds no base for it",
    );
}

/// A pending create the hub offers is staged, not taken as present: until
/// a source's own sync pushes it, its projection says so.
#[test]
fn an_offered_member_reads_as_a_pending_create_until_pushed() {
    let mut mirror = Mirror::new();
    mirror
        .a
        .remote_mut()
        .seed("inbox", "a1", "msg-a", &[], b"body a");
    mirror
        .a
        .sync("inbox", ReplicaSyncOptions::default())
        .unwrap();
    mirror
        .a
        .upgrade(
            "inbox",
            vec![io_replica::placement::ReplicaHandle::from("a1")],
            io_replica::remote::ReplicaTier::Full,
        )
        .unwrap();

    let offered = mirror
        .shared
        .borrow()
        .hub
        .project(&"inbox".into(), &ReplicaSourceId::from("b"));

    assert_eq!(offered.len(), 1, "the hub offers b the member a holds");
    assert_eq!(offered[0].status, ReplicaStatus::Created);
    assert!(
        offered[0].object.is_some(),
        "with the body, so pushing it needs no fetch",
    );
}

/// The storage the other tests lean on behaves like a plain one for a
/// single source, so a difference in the hub tests is the hub's.
#[test]
fn one_source_over_the_hub_matches_the_plain_store() {
    let mut hub = Mirror::new();
    hub.a
        .remote_mut()
        .seed("inbox", "a1", "msg-a", &[], b"body a");
    hub.a
        .remote_mut()
        .seed("inbox", "a2", "msg-b", &["seen"], b"body b");
    let hub_report = hub.a.sync("inbox", ReplicaSyncOptions::default()).unwrap();

    let mut remote = MemRemote::default();
    remote.seed("inbox", "a1", "msg-a", &[], b"body a");
    remote.seed("inbox", "a2", "msg-b", &["seen"], b"body b");
    let mut plain = ReplicaClient::new(MemStorage::default(), remote);
    let plain_report = plain.sync("inbox", ReplicaSyncOptions::default()).unwrap();

    assert_eq!(hub_report, plain_report);
}
