//! Property-based safety net over the conflict axis: two sources, one
//! shared store, and the decisions taken between them.
//!
//! tests/property.rs models one replica against one server and
//! tests/hub_property.rs the cross-source merge; neither drives the
//! conflict lifecycle itself, where every recent consumer data loss came
//! from. This generates edits, flag changes, deletes, refused pushes and
//! resolutions over two sources sharing one hub, and asserts the three
//! laws a resolving consumer is owed rather than a fixed output: the
//! sides converge or something is reported, no body a side held is
//! dropped in silence, and a resolution settles exactly the divergence it
//! was computed against and never one that moved underneath it.

// NOTE: shared across test targets; not every target uses every helper
#[allow(dead_code)]
mod common;

use std::{
    cell::RefCell,
    collections::{BTreeMap, BTreeSet},
    rc::Rc,
};

use io_replica::{
    client::ReplicaClient,
    hub::{ReplicaSourceBinding, ReplicaSourceId},
    mutate::ReplicaMutation,
    object::{ReplicaHash, ReplicaObject},
    placement::{ReplicaFlags, ReplicaHandle, ReplicaLinkId, ReplicaPlacement, ReplicaStatus},
    remote::ReplicaTier,
    sync::{ReplicaSyncOptions, ReplicaSyncReport},
};
use proptest::{prelude::*, test_runner::TestCaseError};

use crate::common::{HubStore, MemRemote, SourceStore, hash};

/// Two sources: the smallest cluster where one side can hold a body the
/// other never saw, which is what a divergence is.
const SOURCES: usize = 2;

/// One step of the conflict scenario. Source and item picks are indices
/// resolved modulo the live sets at execution time, so every generated op
/// is valid by construction and shrinking stays meaningful.
#[derive(Clone, Debug)]
enum ConflictOp {
    /// Stage a local content edit on the i-th item, from the s-th source.
    Edit(usize, usize, u8),
    /// Replace the i-th item's flags from the s-th source.
    SetFlags(usize, usize, ReplicaFlags),
    /// Delete the i-th item on the s-th source, which the hub propagates.
    Remove(usize, usize),
    /// A content edit made on the s-th source's own server, behind the
    /// replica's back: the other half of a per-source divergence.
    ServerEdit(usize, usize, u8),
    /// The s-th source's server starts refusing to accept the i-th item
    /// as an append, the way a DAV collection answers `no-uid-conflict`.
    Refuse(usize, usize),
    /// Resolve the i-th item's conflict on the s-th source with one of
    /// the three bodies it holds, or with a hand-merged fourth.
    Resolve(usize, usize, u8),
    /// Sync and hydrate the s-th source.
    Sync(usize),
}

/// A small flag universe keeps the sets overlapping, which is where the
/// element-wise merge has work to do.
fn arb_flags() -> impl Strategy<Value = ReplicaFlags> {
    proptest::collection::btree_set(prop_oneof![Just("seen"), Just("flagged")], 0..3)
        .prop_map(ReplicaFlags::from_iter)
}

/// Weighted toward what makes a divergence: a conflict needs a local edit
/// and a server edit on one item with no sync between them, and a flat
/// vocabulary generates that far too rarely. `Resolve` carries weight
/// because an unresolved conflict is a dead end for every op after it.
fn arb_conflict_op() -> impl Strategy<Value = ConflictOp> {
    prop_oneof![
        4 => (any::<usize>(), any::<usize>(), any::<u8>())
            .prop_map(|(s, i, n)| ConflictOp::Edit(s, i, n)),
        4 => (any::<usize>(), any::<usize>(), any::<u8>())
            .prop_map(|(s, i, n)| ConflictOp::ServerEdit(s, i, n)),
        3 => (any::<usize>(), any::<usize>(), any::<u8>())
            .prop_map(|(s, i, n)| ConflictOp::Resolve(s, i, n)),
        1 => (any::<usize>(), any::<usize>(), arb_flags())
            .prop_map(|(s, i, f)| ConflictOp::SetFlags(s, i, f)),
        1 => (any::<usize>(), any::<usize>()).prop_map(|(s, i)| ConflictOp::Remove(s, i)),
        1 => (any::<usize>(), any::<usize>()).prop_map(|(s, i)| ConflictOp::Refuse(s, i)),
        2 => any::<usize>().prop_map(ConflictOp::Sync),
    ]
}

/// Two sources over one hub, each with its own mutable-content server, as
/// a two-way consumer wires it.
struct Cluster {
    shared: Rc<RefCell<HubStore>>,
    sources: Vec<ReplicaClient<SourceStore, MemRemote>>,
}

impl Cluster {
    /// A cluster of [`SOURCES`] sources, each seeded with one member of
    /// its own, all mutable-content so an edit has a revision to carry.
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

    /// Syncs and hydrates one source, which is the pass a two-way
    /// consumer runs: a pulled row carries no link id until a meta fetch
    /// resolves it, and a body the hub does not hold it cannot offer.
    fn sync(&mut self, source: usize) -> Option<ReplicaSyncReport> {
        let client = &mut self.sources[source];
        let report = client.sync("inbox", ReplicaSyncOptions::default()).ok()?;
        let opened = client.open("inbox").ok()?;
        let handles = opened.placements.iter().map(|p| p.handle.clone()).collect();
        let _ = client.upgrade("inbox", handles, ReplicaTier::Full);

        Some(report)
    }

    /// Rounds over every source until the hub stops changing, which is
    /// what a two-way consumer's scheduler does.
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

    /// The placement a source projects for `link`, which is what a
    /// consumer reads a conflict from.
    fn placement(&mut self, source: usize, link: &ReplicaLinkId) -> Option<ReplicaPlacement> {
        let opened = self.sources[source].open("inbox").ok()?;
        opened
            .placements
            .into_iter()
            .find(|p| p.link_id.as_ref() == Some(link))
    }

    /// The source's binding of `link`, which holds what its last write
    /// settled: its base, and whether its own sync is stuck.
    fn binding(&self, source: usize, link: &ReplicaLinkId) -> Option<ReplicaSourceBinding> {
        let shared = self.shared.borrow();
        let item = shared.hub.items.get(link)?;
        let source = ReplicaSourceId::from(format!("s{source}"));

        item.sources.get(&source).cloned()
    }

    /// The body a source last synced with its own server for `link`,
    /// which is what its next edit is measured against: an edit
    /// restating it stages nothing at all, so it claims nothing either.
    fn synced_object(&self, source: usize, link: &ReplicaLinkId) -> Option<ReplicaHash> {
        let shared = self.shared.borrow();
        let item = shared.hub.items.get(link)?;
        let source = ReplicaSourceId::from(format!("s{source}"));

        item.sources.get(&source)?.base.as_ref()?.object.clone()
    }

    /// The bytes the store holds for an object, so a resolution can pick
    /// one of the three bodies a conflict is made of.
    fn body(&self, object: &ReplicaHash) -> Option<Vec<u8>> {
        let shared = self.shared.borrow();
        shared.objects.get(object).map(|(_, body)| body.clone())
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

    /// The content revision a source's server reports for `handle`, as
    /// the merge reads it.
    fn server_revision(&self, source: usize, handle: &ReplicaHandle) -> Option<String> {
        self.sources[source]
            .remote()
            .items
            .get(&"inbox".into())?
            .get(handle)
            .map(|item| item.rev.to_string())
    }

    /// Whether the hub reads the item as a cross-source divergence.
    fn conflicted(&self, link: &ReplicaLinkId) -> bool {
        self.shared
            .borrow()
            .hub
            .items
            .get(link)
            .is_some_and(|item| item.conflicted)
    }

    /// Whether the hub still holds the item as live.
    fn live(&self, link: &ReplicaLinkId) -> bool {
        self.shared
            .borrow()
            .hub
            .items
            .get(link)
            .is_some_and(|item| !item.deleted)
    }

    /// Every place a body can still be found: the shared body, either
    /// conflict record, every source's projection and every source's
    /// server. A body in none of them is gone.
    fn holds(&mut self, body: &ReplicaHash) -> bool {
        let recorded = {
            let shared = self.shared.borrow();
            shared.hub.items.values().any(|item| {
                item.object.as_ref() == Some(body)
                    || item.conflict_object.as_ref() == Some(body)
                    || item
                        .sources
                        .values()
                        .any(|binding| binding.conflict_object.as_ref() == Some(body))
            })
        };
        if recorded {
            return true;
        }

        for source in 0..SOURCES {
            let Ok(opened) = self.sources[source].open("inbox") else {
                continue;
            };
            let held = opened.placements.iter().any(|p| {
                p.object.as_ref() == Some(body) || p.conflict_object.as_ref() == Some(body)
            });
            if held {
                return true;
            }
            let handles: Vec<ReplicaHandle> = self.sources[source]
                .remote()
                .items
                .get(&"inbox".into())
                .into_iter()
                .flatten()
                .map(|(handle, _)| handle.clone())
                .collect();
            if handles
                .iter()
                .any(|handle| self.server_object(source, handle).as_ref() == Some(body))
            {
                return true;
            }
        }

        false
    }
}

fn nth<T: Clone>(values: &[T], i: usize) -> Option<T> {
    match values.is_empty() {
        true => None,
        false => values.get(i % values.len()).cloned(),
    }
}

/// The object and bytes of a generated body.
fn object_of(body: &[u8]) -> ReplicaObject {
    ReplicaObject {
        hash: hash(body),
        size: body.len(),
    }
}

/// What the ops asked of one item: the last body a side staged for it,
/// and whether a delete was asked for since.
#[derive(Default)]
struct Owed {
    /// The last body a local edit staged, `None` once an explicit
    /// authority spoke after it (a server-side edit) or the claim was
    /// superseded by a later body.
    body: Option<ReplicaHash>,
    /// Set by a delete, which is a side asking for the body to go.
    removed: bool,
    /// Sources whose own server changed the item without the replica
    /// having folded that change in yet. A body staged while one of
    /// those is pending is owed by nobody: the fold decides between the
    /// two, and which way is the conflict axis, not this law.
    outstanding: BTreeSet<usize>,
}

/// Records that `source` folded its server's state in, so its
/// outstanding server-side edits stop voiding what a side stages next.
fn folded(ledger: &mut BTreeMap<ReplicaLinkId, Owed>, source: usize) {
    for owed in ledger.values_mut() {
        owed.outstanding.remove(&source);
    }
}

/// Resolves a conflict on `source` and asserts the resolution law.
///
/// The consumer decides against three bodies and one observed remote
/// revision. The base the resolution leaves has to be exactly that
/// remote state, both halves of it, and the sync that follows has to
/// settle exactly that divergence: one the remote moved past since is a
/// fresh divergence to report, never a state to overwrite.
///
/// Reports whether there was a conflict to decide, which is all the
/// caller needs: what the decision owes is asserted here, where the
/// state it was computed against is still readable.
fn resolve(
    cluster: &mut Cluster,
    source: usize,
    link: &ReplicaLinkId,
    choice: u8,
) -> Result<bool, TestCaseError> {
    let Some(placement) = cluster.placement(source, link) else {
        return Ok(false);
    };
    if placement.status != ReplicaStatus::Conflict {
        return Ok(false);
    }

    let handle = placement.handle.clone();
    let observed = placement.conflict_revision.clone();
    let diverging = placement.conflict_object.clone();
    let merged = format!("merged-{choice}-{}", link.as_str()).into_bytes();
    let ancestor = placement.base.as_ref().and_then(|b| b.object.clone());

    // NOTE: the four decisions a resolver can take: the ancestor, its
    // own body, the remote's, or a hand-merged fourth.
    let picked = match choice % 4 {
        0 => ancestor.and_then(|object| cluster.body(&object)),
        1 => placement.object.as_ref().and_then(|o| cluster.body(o)),
        2 => diverging.as_ref().and_then(|o| cluster.body(o)),
        _ => None,
    };
    let body = picked.unwrap_or(merged);
    let object = object_of(&body);

    let staged = cluster.sources[source].mutate(
        "inbox",
        ReplicaMutation::Edit {
            handle: handle.clone(),
            object: object.clone(),
            body,
            meta: None,
            sort_key: None,
        },
    );
    if staged.is_err() {
        return Ok(false);
    }

    let binding = cluster
        .binding(source, link)
        .expect("the resolved item is still bound");
    prop_assert!(
        !binding.conflicted,
        "the resolution of {link:?} left s{source} conflicted: {binding:?}",
    );
    prop_assert_eq!(
        binding.base.as_ref().and_then(|b| b.revision.clone()),
        observed.clone(),
        "the base of the resolution is not the revision it was computed against",
    );
    prop_assert_eq!(
        binding.base.as_ref().and_then(|b| b.object.clone()),
        diverging.clone(),
        "the base of the resolution is not the body it was computed against",
    );

    // NOTE: read before the sync, not after: the push moves the
    // revision itself, so only the state the decision goes out against
    // says whether the remote moved under it.
    let held = cluster.server_object(source, &handle);
    let current = cluster.server_revision(source, &handle);
    let (Some(observed), Some(current)) = (observed, current) else {
        cluster.sync(source);
        return Ok(true);
    };

    cluster.sync(source);

    if !cluster.live(link) {
        return Ok(true);
    }
    let Some(binding) = cluster.binding(source, link) else {
        return Ok(true);
    };

    if current != observed {
        // NOTE: the remote moved under the decision, which therefore no
        // longer describes it. The safe answers are to report it anew,
        // or, where the decision kept the remote's own body, to adopt
        // what it holds now. Overwriting it is not one of them.
        prop_assert_eq!(
            cluster.server_object(source, &handle),
            held,
            "a resolution overwrote a remote edit nobody has seen on {:?}",
            link,
        );
        if diverging.as_ref() != Some(&object.hash) {
            prop_assert!(
                binding.conflicted || cluster.conflicted(link),
                "the divergence that moved under the resolution of {link:?} went unreported",
            );
        }
        return Ok(true);
    }

    // NOTE: the divergence is the one the decision was taken against,
    // so the decision settles it: it reached this source's server, or
    // a second divergence across the sources is reported instead.
    if !cluster.conflicted(link) {
        prop_assert_eq!(
            cluster.server_object(source, &binding.handle),
            Some(object.hash.clone()),
            "the resolution of {:?} never reached s{}",
            link,
            source,
        );
    }

    Ok(true)
}

/// Runs the conflict scenario: a seeded cluster, random ops over its two
/// sources, quiescence, then the convergence and no-loss laws.
fn check_conflict_model(ops: Vec<ConflictOp>) -> Result<(), TestCaseError> {
    let mut cluster = Cluster::new();
    cluster.quiesce()?;

    let mut ledger: BTreeMap<ReplicaLinkId, Owed> = BTreeMap::new();

    for op in ops {
        match op {
            ConflictOp::Edit(s, i, n) => {
                let source = s % SOURCES;
                let Some(link) = nth(&cluster.links(), i) else {
                    continue;
                };
                let Some(placement) = cluster.placement(source, &link) else {
                    continue;
                };
                let body = format!("edit-{n}-{}", link.as_str()).into_bytes();
                let object = object_of(&body);
                let stages = cluster.synced_object(source, &link) != Some(object.hash.clone());

                let staged = cluster.sources[source].mutate(
                    "inbox",
                    ReplicaMutation::Edit {
                        handle: placement.handle,
                        object: object.clone(),
                        body,
                        meta: None,
                        sort_key: None,
                    },
                );
                if staged.is_ok() && stages {
                    let owed = ledger.entry(link).or_default();
                    let contested = !owed.outstanding.is_empty();
                    owed.body = (!contested).then_some(object.hash);
                    owed.removed = false;
                }
            }
            ConflictOp::SetFlags(s, i, flags) => {
                let source = s % SOURCES;
                let Some(link) = nth(&cluster.links(), i) else {
                    continue;
                };
                let Some(placement) = cluster.placement(source, &link) else {
                    continue;
                };
                let _ = cluster.sources[source].mutate(
                    "inbox",
                    ReplicaMutation::SetFlags {
                        handle: placement.handle,
                        flags,
                    },
                );
            }
            ConflictOp::Remove(s, i) => {
                let source = s % SOURCES;
                let Some(link) = nth(&cluster.links(), i) else {
                    continue;
                };
                let Some(placement) = cluster.placement(source, &link) else {
                    continue;
                };
                let staged = cluster.sources[source]
                    .mutate("inbox", ReplicaMutation::Remove(placement.handle));
                if staged.is_ok() {
                    ledger.entry(link).or_default().removed = true;
                }
            }
            ConflictOp::ServerEdit(s, i, n) => {
                let source = s % SOURCES;
                let Some(link) = nth(&cluster.links(), i) else {
                    continue;
                };
                let Some(binding) = cluster.binding(source, &link) else {
                    continue;
                };
                if cluster.server_object(source, &binding.handle).is_none() {
                    continue;
                }
                let body = format!("server-{n}-{}", link.as_str()).into_bytes();
                cluster.sources[source]
                    .remote_mut()
                    .edit("inbox", binding.handle.as_str(), &body);
                // NOTE: the server is an authority of its own, and it
                // spoke after whatever a side staged: from here the
                // engine owes a conflict rather than that body.
                let owed = ledger.entry(link).or_default();
                owed.body = None;
                owed.outstanding.insert(source);
            }
            ConflictOp::Refuse(s, i) => {
                let source = s % SOURCES;
                let Some(link) = nth(&cluster.links(), i) else {
                    continue;
                };
                cluster.sources[source]
                    .remote_mut()
                    .refused_appends
                    .insert(link);
            }
            ConflictOp::Resolve(s, i, choice) => {
                let source = s % SOURCES;
                let Some(link) = nth(&cluster.links(), i) else {
                    continue;
                };
                // NOTE: what a resolution owes is asserted where it is
                // taken, against the divergence it was computed against;
                // here it only supersedes whatever the item was owed
                // before, its decision having been made after that.
                if resolve(&mut cluster, source, &link, choice)? {
                    ledger.entry(link).or_default().body = None;
                    folded(&mut ledger, source);
                }
            }
            ConflictOp::Sync(s) => {
                let source = s % SOURCES;
                cluster.sync(source);
                folded(&mut ledger, source);
            }
        }
    }

    cluster.quiesce()?;

    // NOTE: a final pass per source, so one that still owes work says
    // so rather than going quiet.
    let mut reports = Vec::new();
    for source in 0..SOURCES {
        reports.push(cluster.sync(source));
    }

    // NOTE: convergence or a report. A source bound to a live item
    // holds the shared body, or the disagreement is on the record.
    let shared = cluster.shared.borrow().hub.clone();
    for (link, item) in &shared.items {
        if item.deleted {
            continue;
        }
        for (source, binding) in &item.sources {
            let source: usize = source.as_str()[1..].parse().expect("a seeded source id");
            let converged = cluster.server_object(source, &binding.handle) == item.object;
            let reported = item.conflicted
                || binding.conflicted
                || reports[source] != Some(ReplicaSyncReport::default());
            prop_assert!(
                converged || reported,
                "s{source} silently diverges from the shared body of {link:?}: {binding:?}",
            );
        }
    }

    // NOTE: no silent loss. A body a side staged is the shared one, is
    // kept as a recorded divergence, or was superseded by a strictly
    // later action on the same item: a newer body, a server-side edit,
    // or a delete.
    let owed: Vec<(ReplicaLinkId, ReplicaHash)> = ledger
        .iter()
        .filter(|(_, owed)| !owed.removed)
        .filter_map(|(link, owed)| Some((link.clone(), owed.body.clone()?)))
        .collect();
    for (link, body) in owed {
        prop_assert!(
            cluster.holds(&body),
            "the body staged on {link:?} was dropped by nobody's decision",
        );
    }

    Ok(())
}

proptest! {
    /// Whatever the interleaving of local edits, server-side edits, flag
    /// changes, deletes, refused appends and resolutions, every source
    /// converges on the shared body or the disagreement is reported, no
    /// staged body is dropped without a later decision taking its place,
    /// and every resolution settles exactly the divergence it was
    /// computed against.
    #[test]
    fn conflict_interleavings_are_reported_resolved_or_kept(
        ops in proptest::collection::vec(arb_conflict_op(), 0..20),
    ) {
        check_conflict_model(ops)?;
    }
}
