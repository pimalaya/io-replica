//! Property-based safety net over the hub axis: several sources bound to
//! one shared item.
//!
//! The sync axis has a model (tests/property.rs) and the hub had none:
//! its scenarios are hand-scripted, so the cross-source merge is
//! exercised by exactly the interleavings somebody thought of. This
//! drives the same store the hand-written tests use with generated op
//! sequences over three sources, and asserts the four laws a multi-source
//! consumer is owed: every source ends on one body, a source never
//! diverges from itself, a genuine divergence between two sources is
//! reported rather than silently resolved, and no staged body is lost
//! without a strictly later action taking its place.

// NOTE: shared across test targets; not every target uses every helper
#[allow(dead_code)]
mod common;

use std::{cell::RefCell, collections::BTreeMap, rc::Rc};

use io_replica::{
    client::ReplicaClient,
    hub::ReplicaSourceId,
    mutate::ReplicaMutation,
    object::{ReplicaHash, ReplicaObject},
    placement::{ReplicaFlags, ReplicaHandle, ReplicaLinkId},
    remote::ReplicaTier,
    sync::{ReplicaSyncOptions, ReplicaSyncReport},
};
use proptest::{prelude::*, test_runner::TestCaseError};

use crate::common::{HubStore, MemRemote, SourceStore, hash};

/// How many sources the cluster runs. Three rather than two, so a
/// divergence between two of them has a bystander whose binding it must
/// not disturb.
const SOURCES: usize = 3;

/// One step of the hub scenario. Source and item picks are indices
/// resolved modulo the live sets at execution time, so every generated op
/// is valid by construction and shrinking stays meaningful.
#[derive(Clone, Debug)]
enum HubOp {
    /// Stage a local content edit on the i-th shared item, from the s-th
    /// source: the body the hub folds in.
    Edit(usize, usize, u8),
    /// Replace the i-th item's flags from the s-th source.
    SetFlags(usize, usize, ReplicaFlags),
    /// Delete the i-th item on the s-th source, which the hub propagates
    /// to every other.
    Remove(usize, usize),
    /// A new member arrives on the s-th source's server, to be mirrored
    /// to the others.
    ServerAdd(usize, u8),
    /// Stage a locally-authored item on the s-th source, with no remote
    /// origin: the compose or import path, which is a pending create
    /// until a sync pushes it.
    Add(usize, u8),
    /// The i-th item vanishes from the s-th source's server, somebody
    /// else having deleted it there: the delete the hub propagates and a
    /// staged edit resurrects.
    ServerRemove(usize, usize),
    /// Sync and hydrate the s-th source: it pushes what the hub folded in
    /// and folds back what its own server reports.
    Sync(usize),
}

/// A small flag universe keeps the sets overlapping, which is where the
/// element-wise merge has work to do.
fn arb_flags() -> impl Strategy<Value = ReplicaFlags> {
    proptest::collection::btree_set(
        prop_oneof![Just("seen"), Just("flagged"), Just("draft")],
        0..3,
    )
    .prop_map(ReplicaFlags::from_iter)
}

/// Weighted toward the edits, for the reason the sync axis is: a
/// cross-source divergence needs two sources staging different bodies on
/// one item, and a flat vocabulary generates that far too rarely.
fn arb_hub_op() -> impl Strategy<Value = HubOp> {
    prop_oneof![
        4 => (any::<usize>(), any::<usize>(), any::<u8>())
            .prop_map(|(s, i, n)| HubOp::Edit(s, i, n)),
        1 => (any::<usize>(), any::<usize>(), arb_flags())
            .prop_map(|(s, i, f)| HubOp::SetFlags(s, i, f)),
        1 => (any::<usize>(), any::<usize>()).prop_map(|(s, i)| HubOp::Remove(s, i)),
        1 => (any::<usize>(), any::<u8>()).prop_map(|(s, n)| HubOp::ServerAdd(s, n)),
        1 => (any::<usize>(), any::<u8>()).prop_map(|(s, n)| HubOp::Add(s, n)),
        1 => (any::<usize>(), any::<usize>()).prop_map(|(s, i)| HubOp::ServerRemove(s, i)),
        2 => any::<usize>().prop_map(HubOp::Sync),
    ]
}

/// Several sources over one hub, each with its own server, which is how a
/// mirroring consumer wires it.
struct Cluster {
    shared: Rc<RefCell<HubStore>>,
    sources: Vec<ReplicaClient<SourceStore, MemRemote>>,
}

impl Cluster {
    /// A cluster of [`SOURCES`] sources, each seeded with one member of
    /// its own, all of them mutable-content so an edit has a revision to
    /// carry.
    fn new() -> Self {
        let shared = Rc::new(RefCell::new(HubStore::default()));
        let sources = (0..SOURCES)
            .map(|source| {
                let store = SourceStore {
                    source: ReplicaSourceId::from(format!("s{source}")),
                    shared: Rc::clone(&shared),
                };
                let mut remote = MemRemote::default();
                remote.mutable = true;
                remote.seed(
                    "inbox",
                    &format!("h{source}"),
                    &format!("msg-{source}"),
                    &[],
                    format!("seeded on s{source}").as_bytes(),
                );
                ReplicaClient::new(store, remote)
            })
            .collect();

        Self { shared, sources }
    }

    /// Syncs and hydrates one source: the pass that folds what the hub
    /// holds into that source's server and back.
    ///
    /// Hydration is not optional. A pulled row carries no link id, so the
    /// hub cannot key it, and a body the hub does not hold is a member it
    /// cannot offer another source.
    fn sync(&mut self, source: usize) {
        let client = &mut self.sources[source];
        if client.sync("inbox", ReplicaSyncOptions::default()).is_err() {
            return;
        }
        let Ok(opened) = client.open("inbox") else {
            return;
        };
        let handles = opened.placements.iter().map(|p| p.handle.clone()).collect();
        let _ = client.upgrade("inbox", handles, ReplicaTier::Full);
    }

    /// Rounds over every source until the hub stops changing, which is
    /// what a mirroring consumer's scheduler does.
    fn quiesce(&mut self) -> Result<(), TestCaseError> {
        for round in 0..16 {
            let before = self.shared.borrow().hub.clone();
            for source in 0..SOURCES {
                self.sync(source);
            }
            if self.shared.borrow().hub == before {
                return Ok(());
            }
            prop_assert!(round < 15, "the hub never settled");
        }
        Ok(())
    }

    /// The links the hub holds, in order: what an item index picks from.
    fn links(&self) -> Vec<ReplicaLinkId> {
        self.shared.borrow().hub.items.keys().cloned().collect()
    }

    /// The handle a source binds a link under, or `None` when that source
    /// does not hold the item.
    fn handle(&self, source: usize, link: &ReplicaLinkId) -> Option<ReplicaHandle> {
        let shared = self.shared.borrow();
        let item = shared.hub.items.get(link)?;
        let binding = item
            .sources
            .get(&ReplicaSourceId::from(format!("s{source}")))?;

        Some(binding.handle.clone())
    }

    /// The body a source last synced with its own server for `link`,
    /// which is what its next edit is measured against: an edit
    /// restating it stages nothing at all.
    fn synced_object(&self, source: usize, link: &ReplicaLinkId) -> Option<ReplicaHash> {
        let shared = self.shared.borrow();
        let item = shared.hub.items.get(link)?;
        let source = ReplicaSourceId::from(format!("s{source}"));

        item.sources.get(&source)?.base.as_ref()?.object.clone()
    }

    /// The object a source's server holds under `handle`.
    ///
    /// The fake remote records a pushed body as the object's hash written
    /// out and a seeded one as the bytes themselves, so a member that
    /// reached a source through a push and one it was seeded with name
    /// their object differently while meaning the same thing.
    fn server_object(&self, source: usize, handle: &ReplicaHandle) -> Option<ReplicaHash> {
        let body = self.sources[source]
            .remote()
            .items
            .get(&"inbox".into())?
            .get(handle)
            .map(|item| item.body.clone())?;
        let written = ReplicaHash::from(String::from_utf8_lossy(&body).into_owned());

        match self.shared.borrow().objects.contains_key(&written) {
            true => Some(written),
            false => Some(hash(&body)),
        }
    }

    /// The flags a source's server holds under `handle`.
    fn server_flags(&self, source: usize, handle: &ReplicaHandle) -> Option<ReplicaFlags> {
        self.sources[source]
            .remote()
            .items
            .get(&"inbox".into())?
            .get(handle)
            .map(|item| item.flags.clone())
    }
}

/// What the ops asked of one shared item, and what the hub owes for it.
///
/// A shadow of the two axes the cross-source merge reads, kept from the
/// ops alone rather than from the bindings, so a binding that records the
/// wrong agreement point is a failure rather than an agreement.
#[derive(Default)]
struct Owed {
    /// The body the hub shares for the item.
    body: Option<ReplicaHash>,
    /// The source that staged that body, `None` for the body the cluster
    /// started with, which every source already agrees on.
    author: Option<usize>,
    /// The shared body each source last agreed with. A source whose
    /// agreement point is the current shared body has seen it, so its
    /// next edit is made on top of it rather than beside it; one that is
    /// behind stages a body the other never saw.
    agreed: BTreeMap<usize, Option<ReplicaHash>>,
    /// Set once two sources staged different bodies without the second
    /// seeing the first: from there the hub owes a reported conflict
    /// rather than a body.
    diverged: bool,
    /// Set when an edit landed on the item after that divergence, which
    /// is how a consumer resolves one.
    resolved: bool,
    /// Set by a delete, cleared by any later live write.
    removed: bool,
    /// Set when a server-side delete raced the item: whether the delete
    /// propagates or a staged edit resurrects it is what tests/hub.rs
    /// pins by hand, so the body laws step aside for it and only
    /// convergence speaks.
    raced: bool,
}

impl Owed {
    /// Whether the source has seen the body the hub currently shares.
    fn seen(&self, source: usize) -> bool {
        self.agreed.get(&source) == Some(&self.body)
    }

    /// Records that a live write of this source landed: it now agrees
    /// with whatever the reconcile left as the shared body. A tombstone
    /// adopts no content and moves no agreement point, so it never comes
    /// through here.
    fn agree(&mut self, source: usize) {
        self.agreed.insert(source, self.body.clone());
    }
}

/// The ledger of what every generated op asked for, per shared item.
type Ledger = BTreeMap<ReplicaLinkId, Owed>;

fn nth<T: Clone>(values: &[T], i: usize) -> Option<T> {
    match values.is_empty() {
        true => None,
        false => values.get(i % values.len()).cloned(),
    }
}

/// Whether the hub still holds the item as live, which is what a write of
/// a source's placement has to be to move that source's agreement point.
fn live(cluster: &Cluster, link: &ReplicaLinkId) -> bool {
    cluster
        .shared
        .borrow()
        .hub
        .items
        .get(link)
        .is_some_and(|item| !item.deleted)
}

/// Whether the hub reads the item as a cross-source divergence.
fn conflicted(cluster: &Cluster, link: &ReplicaLinkId) -> bool {
    cluster
        .shared
        .borrow()
        .hub
        .items
        .get(link)
        .is_some_and(|item| item.conflicted)
}

/// Runs the hub scenario: a seeded cluster, random ops over its sources,
/// quiescence, then the convergence and ledger assertions.
fn check_hub_model(ops: Vec<HubOp>) -> Result<(), TestCaseError> {
    let mut cluster = Cluster::new();
    cluster.quiesce()?;

    // the cluster starts mirrored, so every source agrees on every body
    let mut ledger: Ledger = cluster
        .shared
        .borrow()
        .hub
        .items
        .iter()
        .map(|(link, item)| {
            let agreed = (0..SOURCES).map(|s| (s, item.object.clone())).collect();
            let owed = Owed {
                body: item.object.clone(),
                agreed,
                ..Owed::default()
            };
            (link.clone(), owed)
        })
        .collect();
    let mut arrivals = 0usize;
    let mut authored = 0usize;

    for op in ops {
        match op {
            HubOp::Edit(s, i, n) => {
                let source = s % SOURCES;
                let Some(link) = nth(&cluster.links(), i) else {
                    continue;
                };
                let Some(handle) = cluster.handle(source, &link) else {
                    continue;
                };
                let body = format!("edit-{n}-{}", link.as_str()).into_bytes();
                let object = ReplicaObject {
                    hash: hash(&body),
                    size: body.len(),
                };
                let owed = ledger.entry(link.clone()).or_default();

                // an edit restating the body this source last synced with
                // its own server stages nothing, so it neither diverges
                // nor resurrects
                let stages = cluster.synced_object(source, &link) != Some(object.hash.clone());

                // a source staging a body over one it never saw, which
                // another source folded in meanwhile, is the genuine
                // cross-source divergence
                let diverging = stages
                    && !owed.seen(source)
                    && owed.body.as_ref().is_some_and(|held| held != &object.hash);
                let first = owed.diverged;
                let was_conflicted = conflicted(&cluster, &link);
                let alone = owed.author.is_none_or(|author| author == source);
                let held = owed.body.clone();

                let staged = cluster.sources[source].mutate(
                    "inbox",
                    ReplicaMutation::Edit {
                        handle,
                        object: object.clone(),
                        body,
                        meta: None,
                        sort_key: None,
                    },
                );
                if staged.is_err() {
                    continue;
                }

                if diverging && !first {
                    let shared = cluster.shared.borrow();
                    let item = shared.hub.items.get(&link).expect("a hubbed item");
                    prop_assert!(
                        item.conflicted,
                        "two sources diverged on {link:?} and the hub resolved it silently: {item:?}",
                    );
                    prop_assert_eq!(
                        item.conflict_object.as_ref(),
                        Some(&object.hash),
                        "the diverging body is what the conflict records",
                    );
                    prop_assert_eq!(
                        item.object.clone(),
                        held.clone(),
                        "and the body it diverged from is kept",
                    );
                } else if !was_conflicted && alone {
                    // nobody else has staged a body here, so a conflict
                    // is this source read as diverging from itself: its
                    // own unpushed edit, or its own resolution, taken for
                    // another source's
                    prop_assert!(
                        !conflicted(&cluster, &link),
                        "s{source} was read as diverging from itself on {link:?}",
                    );
                }

                // the tombstone an edit of a deleted item leaves adopts
                // nothing; any other upsert is live and moves this
                // source's agreement point
                let adopted = live(&cluster, &link);
                let owed = ledger.entry(link).or_default();
                owed.resolved = owed.diverged;
                owed.diverged |= diverging;
                owed.removed &= !stages;

                // a divergence keeps the shared body under `Manual`, and
                // a body the hub already holds is not a move
                if stages && !diverging && owed.body.as_ref() != Some(&object.hash) {
                    owed.body = Some(object.hash);
                    owed.author = Some(source);
                }
                if adopted {
                    owed.agree(source);
                }
            }
            HubOp::SetFlags(s, i, flags) => {
                let source = s % SOURCES;
                let Some(link) = nth(&cluster.links(), i) else {
                    continue;
                };
                let Some(handle) = cluster.handle(source, &link) else {
                    continue;
                };
                let staged = cluster.sources[source]
                    .mutate("inbox", ReplicaMutation::SetFlags { handle, flags });
                if staged.is_ok() && live(&cluster, &link) {
                    ledger.entry(link).or_default().agree(source);
                }
            }
            HubOp::Remove(s, i) => {
                let source = s % SOURCES;
                let Some(link) = nth(&cluster.links(), i) else {
                    continue;
                };
                let Some(handle) = cluster.handle(source, &link) else {
                    continue;
                };
                let staged =
                    cluster.sources[source].mutate("inbox", ReplicaMutation::Remove(handle));
                if staged.is_ok() {
                    // NOTE: a tombstone adopts no content, so it moves
                    // neither the shared body nor this source's agreement
                    // point: the delete is the only thing it claims.
                    ledger.entry(link).or_default().removed = true;
                }
            }
            HubOp::ServerAdd(s, n) => {
                arrivals += 1;
                let source = s % SOURCES;
                cluster.sources[source].remote_mut().seed(
                    "inbox",
                    &format!("srv-{arrivals}"),
                    &format!("lnk-{arrivals}"),
                    &[],
                    format!("arrival-{n}").as_bytes(),
                );
            }
            HubOp::ServerRemove(s, i) => {
                let source = s % SOURCES;
                let Some(link) = nth(&cluster.links(), i) else {
                    continue;
                };
                let Some(handle) = cluster.handle(source, &link) else {
                    continue;
                };
                if cluster.server_object(source, &handle).is_none() {
                    continue;
                }
                cluster.sources[source]
                    .remote_mut()
                    .remove("inbox", handle.as_str());
                // the body claims step aside, the agreement points the
                // divergence laws read do not: a delete on one server
                // moves no shared body
                ledger.entry(link).or_default().raced = true;
            }
            HubOp::Add(s, n) => {
                authored += 1;
                let source = s % SOURCES;
                let link = ReplicaLinkId::from(format!("new-{authored}"));
                let body = format!("authored-{n}-{authored}").into_bytes();
                let object = ReplicaObject {
                    hash: hash(&body),
                    size: body.len(),
                };
                let staged = cluster.sources[source].mutate(
                    "inbox",
                    ReplicaMutation::Add {
                        handle: ReplicaHandle::from(format!("tmp-{authored}")),
                        link_id: link.clone(),
                        flags: ReplicaFlags::default(),
                        object: object.clone(),
                        body,
                        meta: None,
                        sort_key: Default::default(),
                    },
                );
                if staged.is_ok() {
                    let owed = ledger.entry(link).or_default();
                    owed.body = Some(object.hash);
                    owed.author = Some(source);
                    owed.agree(source);
                }
            }
            HubOp::Sync(s) => {
                let source = s % SOURCES;
                cluster.sync(source);
                // the sync writes back every live item it holds, and a
                // deleted one only as the tombstone it pushes, which
                // adopts nothing
                let live: Vec<ReplicaLinkId> = cluster
                    .links()
                    .into_iter()
                    .filter(|link| live(&cluster, link))
                    .collect();
                for link in live {
                    ledger.entry(link).or_default().agree(source);
                }
            }
        }
    }

    cluster.quiesce()?;

    // convergence: every source bound to an item holds it under its own
    // handle, at the one shared body and the one shared flag set
    let shared = cluster.shared.borrow().hub.clone();
    for (link, item) in &shared.items {
        if item.deleted {
            continue;
        }
        for (source, binding) in &item.sources {
            let source: usize = source.as_str()[1..].parse().expect("a seeded source id");
            prop_assert_eq!(
                cluster.server_object(source, &binding.handle),
                item.object.clone(),
                "s{} diverges from the shared body of {:?}",
                source,
                link,
            );
            prop_assert_eq!(
                cluster.server_flags(source, &binding.handle),
                Some(item.flags.clone()),
                "s{} diverges from the shared flags of {:?}",
                source,
                link,
            );
        }
    }

    // completeness: every staged body became the shared one, is held as a
    // reported conflict, or was superseded by a strictly later action on
    // the same item, which is a delete, a newer body, or a delete on some
    // source's own server
    for (link, owed) in &ledger {
        let item = shared.items.get(link);
        if owed.raced {
            continue;
        }
        if owed.removed {
            prop_assert!(
                item.is_none_or(|item| item.deleted),
                "the delete of {link:?} was undone: {item:?}",
            );
            continue;
        }
        if owed.diverged {
            prop_assert!(
                owed.resolved || item.is_none_or(|item| item.conflicted),
                "the divergence on {link:?} was resolved by nobody: {item:?}",
            );
            continue;
        }
        let Some(staged) = &owed.body else {
            continue;
        };
        let item = item.expect("the edited item is still hubbed");
        prop_assert_eq!(
            item.object.as_ref(),
            Some(staged),
            "the body staged on {:?} never became the shared one",
            link,
        );
    }

    // idempotence: a quiescent source syncs to nothing
    for source in 0..SOURCES {
        let report = cluster.sources[source]
            .sync("inbox", ReplicaSyncOptions::default())
            .expect("a quiescent sync");
        prop_assert_eq!(
            report,
            ReplicaSyncReport::default(),
            "s{} is not settled",
            source,
        );
    }
    Ok(())
}

proptest! {
    /// Whatever the interleaving of per-source edits, flag changes,
    /// deletes, arrivals and syncs, the sources converge on one body per
    /// item, no source is ever read as diverging from itself, a genuine
    /// divergence between two sources is reported rather than resolved,
    /// and no staged body goes missing.
    #[test]
    fn hub_interleavings_converge_across_sources(
        ops in proptest::collection::vec(arb_hub_op(), 0..25),
    ) {
        check_hub_model(ops)?;
    }
}
