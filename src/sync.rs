//! I/O-free coroutine to reconcile a collection with its remote.
//!
//! The load-bearing verb. It loads local state, enumerates the remote
//! delta, then runs a three-way merge of local, base and remote per
//! placement: local-won changes are pushed, remote-won changes are
//! pulled. Flags merge element-wise and never conflict (each flag is
//! independent, so divergent sets fold into their union of changes);
//! only divergent content edits are kept both as a conflict. The merge
//! compares per-placement identities (flags and a content revision),
//! never raw bytes: the complete probed spine plus the per-placement
//! base, not a missing body, is what tells deleted from not-cached.
//!
//! Backends where the content itself is mutable (an item can be edited in
//! place) carry that mutation in the content revision; backends where it
//! is immutable carry only flag and membership changes. Either way the
//! merge shape is the same. An edit beats a delete in both directions: a
//! remote update resurrects a local tombstone, and a local staged edit
//! survives a remote delete as a pending create. Permission gating drops
//! pushes a read-only source forbids.

use core::mem;

use alloc::{collections::BTreeMap, collections::BTreeSet, string::String, vec::Vec};

use log::{debug, trace};
use thiserror::Error;

use crate::{
    change::{ReplicaChange, ReplicaWriteOp},
    collection::{ReplicaCheckpoint, ReplicaCollectionId},
    coroutine::*,
    placement::{
        ReplicaBase, ReplicaFlags, ReplicaHandle, ReplicaLevel, ReplicaPlacement, ReplicaSortKey,
        ReplicaStatus,
    },
    remote::{ReplicaPushOutcome, ReplicaRemoteItem, ReplicaRemoteSnapshot},
};

/// Which push kinds a writable source may derive, refining
/// [`ReplicaSyncOptions::push`].
///
/// Each kind is independent, so a source can, for example, accept flag
/// changes but refuse deletes. All permitted by default, so the refinement is
/// opt-in and leaves the all-or-nothing behaviour unchanged. Only consulted
/// when [`ReplicaSyncOptions::push`] is true; a false `push` is fully
/// read-only regardless of these.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReplicaPushRights {
    /// May push a flag-set change.
    pub flags: bool,
    /// May push an in-place content update.
    pub content: bool,
    /// May push a membership add (a create: copy, move target or append).
    pub add: bool,
    /// May push a membership remove (a delete or move source).
    pub remove: bool,
}

impl ReplicaPushRights {
    /// Every push kind permitted (the default).
    pub const fn all() -> Self {
        Self {
            flags: true,
            content: true,
            add: true,
            remove: true,
        }
    }

    /// No push kind permitted.
    pub const fn none() -> Self {
        Self {
            flags: false,
            content: false,
            add: false,
            remove: false,
        }
    }
}

impl Default for ReplicaPushRights {
    fn default() -> Self {
        Self::all()
    }
}

/// How a headless sync resolves a content conflict — content diverged on both
/// sides against a shared base.
///
/// Only mutable-content backends can conflict (immutable content reports no
/// revision, so the merge never sees a divergence). An interactive consumer
/// leaves this `Manual` and resolves with an edit; an unattended sync picks one
/// of the automatic policies. A base-less create-collision (both sides minted
/// content for one handle with no shared ancestor) is always kept as a conflict
/// regardless of this policy: there is no three-way merge to automate.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum ReplicaConflictPolicy {
    /// Mark the placement conflicted and leave it for the consumer to resolve
    /// with an edit (the interactive default).
    #[default]
    Manual,
    /// Keep the local edit, overwriting the remote's current version.
    PreferLocal,
    /// Keep the remote edit, dropping the local one and pulling the remote.
    PreferRemote,
    /// Keep both: pull the remote into the placement and stage the local body
    /// as a new member (a create the next sync appends), so neither is lost.
    KeepBoth,
}

/// Tuning for one sync run: the push direction and the enumerate depth.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReplicaSyncOptions {
    /// The master push switch. When false the source is treated read-only:
    /// local flag and content changes are kept dirty and never pushed
    /// (permission gating). Local deletes instead apply locally only,
    /// deliberately: they can never propagate, so the replica mirrors the
    /// source and the next enumerate re-adds the member.
    pub push: bool,
    /// Per-kind refinement of `push`, consulted only when `push` is true. A
    /// forbidden push kind is kept pending (never pushed, and a forbidden
    /// delete is not applied to the replica either), while other kinds still
    /// propagate.
    pub rights: ReplicaPushRights,
    /// How a content conflict is resolved when the source is unattended.
    /// Defaults to `Manual` (mark and wait for an edit).
    pub conflict: ReplicaConflictPolicy,
    /// When true the checkpoint is ignored and the whole remote is
    /// enumerated, so the merge reconciles the complete spine: it re-adds
    /// any locally-missing message and drops any local phantom. The
    /// recovery path for a replica that drifted out of sync.
    pub full: bool,
}

impl ReplicaSyncOptions {
    /// Whether a push of the kind selected by `right` may be derived: the
    /// master switch on, and the specific right permitted.
    fn may_push(&self, right: impl Fn(&ReplicaPushRights) -> bool) -> bool {
        self.push && right(&self.rights)
    }
}

impl Default for ReplicaSyncOptions {
    fn default() -> Self {
        Self {
            push: true,
            rights: ReplicaPushRights::all(),
            conflict: ReplicaConflictPolicy::Manual,
            full: false,
        }
    }
}

/// A per-item outcome of a sync, for hooks and richer reporting.
///
/// Emitted as the merge (and the push confirmation) touches each handle: what
/// changed for that placement, in order. Watch/notify/log hooks ride these; the
/// counters on [`ReplicaSyncReport`] summarise them. Events are data, spine
/// level (a handle, no body), so emitting them enters no I/O.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ReplicaEvent {
    /// A new member appeared locally, pulled from the remote.
    Added(ReplicaHandle),
    /// A placement's flag set changed (pulled from, or pushed to, the remote).
    FlagsChanged(ReplicaHandle),
    /// A placement's content changed: a stale body dropped for an on-demand
    /// refetch (pull), or a local edit the remote confirmed (push).
    ContentChanged(ReplicaHandle),
    /// A member was removed: dropped after a remote delete, or a local delete
    /// the remote confirmed.
    Vanished(ReplicaHandle),
    /// A placement's content diverged on both sides and is left conflicted.
    Conflicted(ReplicaHandle),
    /// A local create (copy, move target or append) the remote accepted.
    Created(ReplicaHandle),
}

/// What a sync did.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ReplicaSyncReport {
    /// Placements changed by pulling the remote.
    pub pulled: usize,
    /// Changes the remote accepted.
    pub pushed: usize,
    /// Placements left in conflict (content diverged on both sides).
    pub conflicts: usize,
    /// Pushes the remote rejected on optimistic concurrency.
    pub rejected: usize,
    /// Placements whose stale body was dropped after a remote content
    /// change; an upgrade refetches them on demand.
    pub refreshed: usize,
    /// The per-item events this sync emitted, in order.
    pub events: Vec<ReplicaEvent>,
}

/// Failure causes during a SYNC flow.
#[derive(Clone, Debug, Error)]
pub enum ReplicaSyncError {
    /// The driver fed back an arg that does not match the pending yield.
    #[error("Replica SYNC failed: unexpected coroutine arg")]
    UnexpectedArg,
    /// The driver resumed without the arg the pending yield required.
    #[error("Replica SYNC failed: missing coroutine arg")]
    MissingArg,
}

/// I/O-free SYNC coroutine.
pub struct ReplicaSync {
    collection: ReplicaCollectionId,
    opts: ReplicaSyncOptions,
    local: BTreeMap<ReplicaHandle, ReplicaPlacement>,
    checkpoint: Option<ReplicaCheckpoint>,
    writes: Vec<ReplicaWriteOp>,
    /// Flag pushes awaiting their outcome: the local placement to rebase
    /// once the remote confirms, keyed by handle. A rejected push leaves
    /// the placement untouched (dirty) so the next sync retries it.
    pending_flag_pushes: BTreeMap<ReplicaHandle, ReplicaPlacement>,
    /// Content pushes awaiting their outcome: the local placement whose
    /// base adopts the pushed body and reported revision once the remote
    /// confirms. Kept apart from flag pushes so an accepted flag push on
    /// a conflicted placement (whose body also differs from its base) is
    /// never misread as a resolved content push.
    pending_content_pushes: BTreeMap<ReplicaHandle, ReplicaPlacement>,
    /// Tombstone deletes awaiting their outcome: the placement is dropped
    /// only once the remote confirms the delete. A rejected push keeps the
    /// tombstone so the next sync retries, rather than dropping a message
    /// the server still has (a permanent desync under incremental sync).
    pending_drops: BTreeSet<ReplicaHandle>,
    /// Pending creates awaiting their outcome, keyed by provisional handle:
    /// the staged placement to rekey to the server-assigned handle once the
    /// add is accepted. A rejected push keeps the placeholder for a retry.
    pending_creates: BTreeMap<ReplicaHandle, ReplicaPlacement>,
    report: ReplicaSyncReport,
    state: State,
}

impl ReplicaSync {
    /// Creates a coroutine that reconciles `collection`.
    pub fn new(collection: impl Into<ReplicaCollectionId>, opts: ReplicaSyncOptions) -> Self {
        let collection = collection.into();
        debug!(
            "sync collection {} (push={})",
            collection.as_str(),
            opts.push
        );

        Self {
            collection,
            opts,
            local: BTreeMap::new(),
            checkpoint: None,
            writes: Vec::new(),
            pending_flag_pushes: BTreeMap::new(),
            pending_content_pushes: BTreeMap::new(),
            pending_drops: BTreeSet::new(),
            pending_creates: BTreeMap::new(),
            report: ReplicaSyncReport::default(),
            state: State::Start,
        }
    }

    /// Runs the three-way merge, filling `self.writes` and `self.report`
    /// and returning the pushes to send.
    fn reconcile(&mut self, snapshot: ReplicaRemoteSnapshot) -> Vec<ReplicaChange> {
        let ReplicaRemoteSnapshot {
            items,
            vanished,
            complete,
            checkpoint,
        } = snapshot;

        let remote: BTreeMap<ReplicaHandle, ReplicaRemoteItem> =
            items.into_iter().map(|i| (i.handle.clone(), i)).collect();
        let vanished: BTreeSet<ReplicaHandle> = vanished.into_iter().collect();

        let candidates = if complete {
            self.full_candidates(&remote)
        } else {
            self.delta_candidates(&remote, &vanished)
        };

        let mut pushes = Vec::new();

        for (handle, remote_item) in candidates {
            if let Some(change) = self.merge(&handle, remote_item) {
                pushes.push(change);
            }
        }

        self.writes.push(ReplicaWriteOp::SetCheckpoint {
            collection: self.collection.clone(),
            checkpoint,
        });

        pushes
    }

    /// The handles to merge for a complete snapshot, each paired with its
    /// remote state: the union of local and remote handles, where a local
    /// handle absent from `remote` reads as removed upstream.
    fn full_candidates(
        &self,
        remote: &BTreeMap<ReplicaHandle, ReplicaRemoteItem>,
    ) -> Vec<(ReplicaHandle, Option<ReplicaRemoteItem>)> {
        let handles: BTreeSet<ReplicaHandle> =
            self.local.keys().chain(remote.keys()).cloned().collect();
        handles
            .into_iter()
            .map(|handle| {
                let item = remote.get(&handle).cloned();
                (handle, item)
            })
            .collect()
    }

    /// The handles to merge for a delta snapshot: the changed handles, the
    /// vanished ones, and any locally dirty handle (whose pending push the
    /// delta would otherwise never revisit). An unlisted dirty handle is
    /// unchanged upstream, so its remote state is its own base.
    fn delta_candidates(
        &self,
        remote: &BTreeMap<ReplicaHandle, ReplicaRemoteItem>,
        vanished: &BTreeSet<ReplicaHandle>,
    ) -> Vec<(ReplicaHandle, Option<ReplicaRemoteItem>)> {
        let dirty = self
            .local
            .iter()
            .filter(|(_, p)| p.status != ReplicaStatus::Clean)
            .map(|(handle, _)| handle.clone());

        let handles: BTreeSet<ReplicaHandle> = remote
            .keys()
            .cloned()
            .chain(vanished.iter().cloned())
            .chain(dirty)
            .collect();

        handles
            .into_iter()
            .map(|handle| {
                let item = if vanished.contains(&handle) {
                    None
                } else if let Some(item) = remote.get(&handle) {
                    Some(item.clone())
                } else {
                    self.local.get(&handle).and_then(|p| {
                        let base = p.base.as_ref()?;
                        Some(ReplicaRemoteItem {
                            handle: handle.clone(),
                            flags: base.flags.clone(),
                            // NOTE: the best-known remote state: a
                            // conflicted placement has observed a remote
                            // revision past its base, and synthesizing the
                            // base one would regress its conflict tracking
                            revision: p
                                .conflict_revision
                                .clone()
                                .or_else(|| base.revision.clone()),
                        })
                    })
                };
                (handle, item)
            })
            .collect()
    }

    /// Three-way merges one handle against its remote state, writing the
    /// resolved placement and returning a push when the local side won.
    fn merge(
        &mut self,
        handle: &ReplicaHandle,
        remote_item: Option<ReplicaRemoteItem>,
    ) -> Option<ReplicaChange> {
        let local = self.local.get(handle).cloned();

        let based = local.as_ref().map(|p| p.base.is_some()).unwrap_or(false);

        let local_tombstone = local
            .as_ref()
            .map(|p| p.status == ReplicaStatus::Tombstone)
            .unwrap_or(false);
        let local_present = local.is_some() && !local_tombstone;
        let remote_present = remote_item.is_some();

        match (local_present, based, remote_present) {
            // NOTE: local delete or move: both knew it, we removed it here
            (false, true, true) if local_tombstone => {
                let local = local.as_ref().expect("local present");
                let item = remote_item.as_ref().expect("remote present");

                // NOTE: the remote edited what was deleted locally: the
                // update wins over the delete, so the tombstone is replaced
                // by a fresh pull of the new revision (body on demand).
                let base_revision = local.base.as_ref().and_then(|b| b.revision.clone());
                if item.revision.is_some() && item.revision != base_revision {
                    self.pull_add(item);
                    self.report.pulled += 1;
                    self.emit(ReplicaEvent::Added(handle.clone()));
                    return None;
                }

                if !self.opts.push {
                    // NOTE: read-only source: apply the delete locally only.
                    self.drop(handle);
                    return None;
                }

                // NOTE: a staged content edit rides ahead of a move: the
                // update pushes first, so the relocated member carries the
                // edited body; the move itself derives again on the next
                // sync, once the base holds the pushed content. A plain
                // delete supersedes the edit instead.
                let edited = local.object.is_some()
                    && local
                        .base
                        .as_ref()
                        .is_some_and(|b| local.object != b.object);
                if edited && local.origin.is_some() {
                    if !self.opts.rights.content {
                        // NOTE: content push forbidden, keep the tombstone
                        // pending.
                        return None;
                    }
                    self.pending_content_pushes
                        .insert(handle.clone(), local.clone());
                    return Some(ReplicaChange::Update {
                        handle: handle.clone(),
                        object: local.object.clone().expect("a staged edited body"),
                        if_match: base_revision,
                    });
                }

                if !self.opts.rights.remove {
                    // NOTE: remove push forbidden, keep the tombstone pending,
                    // and do not drop it locally (unlike a read-only source,
                    // which mirrors the delete).
                    return None;
                }

                // NOTE: hold the drop until the remote confirms it (in
                // PendingPush); a rejected push keeps the tombstone for the
                // next retry. A move carries its destination in `origin`; a
                // plain delete has none, and the consumer routes it to trash.
                self.pending_drops.insert(handle.clone());
                let to = local.origin.as_ref().map(|o| o.collection.clone());
                Some(ReplicaChange::Remove {
                    handle: handle.clone(),
                    to,
                    if_match: base_revision,
                })
            }
            // NOTE: local delete of something already gone remote
            (false, _, false) if local_tombstone => {
                self.drop(handle);
                None
            }
            // NOTE: remote delete: we had it in sync, it vanished upstream
            (true, true, false) => {
                let local = local.as_ref().expect("local present");

                // NOTE: the edit-beats-delete rule, push direction: a
                // staged local edit survives the remote delete as a
                // pending create that re-uploads the edited body, rather
                // than being silently dropped with the placement. A
                // conflicted placement holds such an edit too, and the
                // remote deleting its side makes the conflict moot: the
                // surviving local edit wins by default.
                let edited = matches!(local.status, ReplicaStatus::Dirty | ReplicaStatus::Conflict)
                    && local.object.is_some()
                    && local
                        .base
                        .as_ref()
                        .is_some_and(|b| local.object != b.object);
                if edited {
                    let mut resurrected = local.clone();
                    resurrected.status = ReplicaStatus::Created;
                    resurrected.conflict_revision = None;
                    resurrected.base = None;
                    resurrected.origin = None;
                    self.writes
                        .push(ReplicaWriteOp::UpsertPlacement(resurrected.clone()));

                    if !self.opts.may_push(|r| r.add) {
                        // NOTE: read-only or add forbidden, keep the
                        // resurrected create staged and pending, do not push.
                        return None;
                    }
                    self.pending_creates.insert(handle.clone(), resurrected);
                    return Some(ReplicaChange::Add {
                        handle: handle.clone(),
                        link_id: local.link_id.clone(),
                        flags: local.flags.clone(),
                        origin: None,
                        object: local.object.clone(),
                    });
                }

                self.drop(handle);
                self.report.pulled += 1;
                self.emit(ReplicaEvent::Vanished(handle.clone()));
                None
            }
            // NOTE: remote add: brand new upstream
            (false, false, true) => {
                self.pull_add(&remote_item.expect("remote present"));
                self.report.pulled += 1;
                self.emit(ReplicaEvent::Added(handle.clone()));
                None
            }
            // NOTE: present on both: reconcile content, then flags
            (true, _, true) => {
                let local = local.as_ref().expect("local present");
                let item = remote_item.as_ref().expect("remote present");

                // NOTE: the content axis may have rewritten the placement
                // (a pull, a conflict mark, conflict tracking); flags
                // reconcile on the rewritten copy so both changes land in
                // one write chain. A remote flag change must survive even
                // a content conflict: a delta lists it exactly once, so
                // skipping the flag merge here would lose it for good.
                match self.reconcile_content(local, item) {
                    ContentOutcome::Push(change) => Some(change),
                    ContentOutcome::Rewritten(rewritten) => self.reconcile_flags(&rewritten, item),
                    ContentOutcome::Untouched => self.reconcile_flags(local, item),
                }
            }
            // NOTE: local create (no base, not upstream): a pending copy,
            // move or append. Stage the add; the rekey to the server-assigned
            // handle is held until the push reports it (in PendingPush). A
            // plain base-less placement that is not Created is left untouched.
            (true, false, false) => {
                let local = local.as_ref().expect("local present");
                if self.opts.may_push(|r| r.add) && local.status == ReplicaStatus::Created {
                    self.pending_creates.insert(handle.clone(), local.clone());
                    return Some(ReplicaChange::Add {
                        handle: handle.clone(),
                        link_id: local.link_id.clone(),
                        flags: local.flags.clone(),
                        origin: local.origin.clone(),
                        object: local.object.clone(),
                    });
                }
                None
            }
            _ => None,
        }
    }

    /// Reconciles the content of a placement present on both sides, ahead
    /// of the flag reconciliation.
    ///
    /// Both axes are read from positive signals only: the local side
    /// changed when a dirty placement points at a body its base does not
    /// hold (a staged edit), the remote side when the enumerate reported a
    /// revision differing from the base. An immutable-content backend
    /// produces neither, so it always falls through untouched to the
    /// flags-only merge.
    fn reconcile_content(
        &mut self,
        local: &ReplicaPlacement,
        item: &ReplicaRemoteItem,
    ) -> ContentOutcome {
        let Some(base) = &local.base else {
            // NOTE: a base-less placement that carries a body the remote also
            // holds is a create-collision: both sides minted content for
            // this handle with no shared ancestor, so there is no
            // three-way merge to run. Surface it as a conflict (the remote
            // revision is what the consumer resolves against) instead of
            // converging on flags alone, which would strand the two bodies
            // apart and, once a spoke's base is lost this way, loop every
            // sync without ever reconciling the content again. A body-less
            // never-based placement carries no content signal, so it still
            // falls through to the flag reconciliation.
            if local.object.is_some() && item.revision.is_some() {
                let mut conflicted = local.clone();
                conflicted.status = ReplicaStatus::Conflict;
                conflicted.conflict_revision = item.revision.clone();
                self.writes
                    .push(ReplicaWriteOp::UpsertPlacement(conflicted.clone()));
                self.report.conflicts += 1;
                self.emit(ReplicaEvent::Conflicted(local.handle.clone()));
                return ContentOutcome::Rewritten(conflicted);
            }
            return ContentOutcome::Untouched;
        };

        if local.status == ReplicaStatus::Conflict {
            // NOTE: an unresolved conflict is left alone (the local edit
            // must survive), but the observed remote revision is kept
            // current so the consumer resolves against the latest remote.
            if item.revision.is_some() && item.revision != local.conflict_revision {
                let mut updated = local.clone();
                updated.conflict_revision = item.revision.clone();
                self.writes
                    .push(ReplicaWriteOp::UpsertPlacement(updated.clone()));
                return ContentOutcome::Rewritten(updated);
            }
            return ContentOutcome::Untouched;
        }

        let local_changed = local.status == ReplicaStatus::Dirty
            && local.object.is_some()
            && local.object != base.object;
        let remote_changed = item.revision.is_some() && item.revision != base.revision;

        match (local_changed, remote_changed) {
            (false, false) => ContentOutcome::Untouched,
            (false, true) => ContentOutcome::Rewritten(self.pull_content(local, item)),
            (true, false) => {
                if !self.opts.may_push(|r| r.content) {
                    // NOTE: read-only source or content push forbidden, keep
                    // dirty and do not push
                    return ContentOutcome::Untouched;
                }
                self.pending_content_pushes
                    .insert(local.handle.clone(), local.clone());
                ContentOutcome::Push(ReplicaChange::Update {
                    handle: local.handle.clone(),
                    object: local.object.clone().expect("a staged edited body"),
                    if_match: base.revision.clone(),
                })
            }
            (true, true) => self.resolve_conflict(local, item),
        }
    }

    /// Resolves a content conflict (both sides edited a based placement) by the
    /// configured [`ReplicaConflictPolicy`].
    fn resolve_conflict(
        &mut self,
        local: &ReplicaPlacement,
        item: &ReplicaRemoteItem,
    ) -> ContentOutcome {
        match self.opts.conflict {
            ReplicaConflictPolicy::Manual => self.mark_conflict(local, item),
            ReplicaConflictPolicy::PreferRemote => {
                ContentOutcome::Rewritten(self.pull_content(local, item))
            }
            ReplicaConflictPolicy::PreferLocal => {
                // NOTE: overwrite the remote's *current* version, so the
                // precondition is the observed remote revision, not the stale
                // base (which would be rejected on optimistic concurrency).
                if !self.opts.may_push(|r| r.content) {
                    return self.mark_conflict(local, item);
                }
                self.pending_content_pushes
                    .insert(local.handle.clone(), local.clone());
                ContentOutcome::Push(ReplicaChange::Update {
                    handle: local.handle.clone(),
                    object: local.object.clone().expect("a staged edited body"),
                    if_match: item.revision.clone(),
                })
            }
            ReplicaConflictPolicy::KeepBoth => {
                self.stage_conflict_dup(local);
                ContentOutcome::Rewritten(self.pull_content(local, item))
            }
        }
    }

    /// Marks a placement conflicted, carrying the observed remote revision for
    /// the consumer to resolve against.
    fn mark_conflict(
        &mut self,
        local: &ReplicaPlacement,
        item: &ReplicaRemoteItem,
    ) -> ContentOutcome {
        let mut conflicted = local.clone();
        conflicted.status = ReplicaStatus::Conflict;
        conflicted.conflict_revision = item.revision.clone();
        self.writes
            .push(ReplicaWriteOp::UpsertPlacement(conflicted.clone()));
        self.report.conflicts += 1;
        self.emit(ReplicaEvent::Conflicted(local.handle.clone()));
        ContentOutcome::Rewritten(conflicted)
    }

    /// Stages the local body as a fresh `Created` member so a `KeepBoth`
    /// resolution loses neither version; the next sync appends it.
    fn stage_conflict_dup(&mut self, local: &ReplicaPlacement) {
        let Some(object) = local.object.clone() else {
            return;
        };
        let mut handle = local.handle.0.clone();
        handle.push_str("\u{1}keepboth");
        let dup = ReplicaPlacement {
            collection: self.collection.clone(),
            handle: ReplicaHandle(handle),
            link_id: None,
            object: Some(object),
            level: ReplicaLevel::Full,
            meta: local.meta.clone(),
            sort_key: local.sort_key.clone(),
            flags: local.flags.clone(),
            status: ReplicaStatus::Created,
            conflict_revision: None,
            base: None,
            origin: None,
        };
        self.writes.push(ReplicaWriteOp::UpsertPlacement(dup));
    }

    /// Reconciles the flag sets of a placement present on both sides,
    /// returning a push when the local side won any flag.
    ///
    /// Flags merge element-wise ([`ReplicaFlags::merge`]) and never
    /// conflict: divergent sets fold into one merged set that both sides
    /// converge on, pulling the remote-won flags and pushing the local-won
    /// ones.
    fn reconcile_flags(
        &mut self,
        local: &ReplicaPlacement,
        remote: &ReplicaRemoteItem,
    ) -> Option<ReplicaChange> {
        let base_flags = local.base.as_ref().map(|b| b.flags.clone());

        let Some(base_flags) = base_flags else {
            // NOTE: never based but present on both: converge on remote
            self.pull_flags(local, &remote.flags);
            self.report.pulled += 1;
            self.emit(ReplicaEvent::FlagsChanged(local.handle.clone()));
            return None;
        };

        let merged = ReplicaFlags::merge(&base_flags, &local.flags, &remote.flags);
        let pull = merged != local.flags;
        let push = merged != remote.flags;

        match (pull, push) {
            // NOTE: in sync, or both sides moved to the same flags: no
            // push, rebase when the shared move left the base behind, and
            // clean a dirty placement whose edit turned out to be a no-op
            // (a flag set put back to its base value would otherwise stay
            // dirty forever). A staged content edit is not a no-op: it
            // stays dirty (the read-only path lands here with one).
            (false, false) => {
                let content_edit = local.object.is_some()
                    && local
                        .base
                        .as_ref()
                        .is_some_and(|b| b.object != local.object);
                if local.flags != base_flags
                    || (local.status == ReplicaStatus::Dirty && !content_edit)
                {
                    self.rebase(local, &merged);
                }
                None
            }
            // NOTE: the remote won every differing flag: pull the merged set
            (true, false) => {
                self.pull_flags(local, &merged);
                self.report.pulled += 1;
                self.emit(ReplicaEvent::FlagsChanged(local.handle.clone()));
                None
            }
            // NOTE: the local side won at least one flag: push the merged
            // set. When the remote won some flags too, they are folded in
            // locally right away, dirty on the old base, so a rejected or
            // read-only push re-derives the same merge next sync.
            (pull, true) => {
                let mut updated = local.clone();
                updated.flags = merged.clone();
                if pull {
                    self.writes
                        .push(ReplicaWriteOp::UpsertPlacement(updated.clone()));
                    self.report.pulled += 1;
                    self.emit(ReplicaEvent::FlagsChanged(local.handle.clone()));
                }

                if !self.opts.may_push(|r| r.flags) {
                    // NOTE: read-only source or flag push forbidden, keep dirty
                    // and do not push
                    return None;
                }
                // NOTE: hold the rebase until the push is confirmed: a
                // rejected push must leave the placement dirty so the next
                // sync retries it, not rebase it onto a state the remote
                // never took (which QRESYNC would then never revisit).
                self.pending_flag_pushes
                    .insert(local.handle.clone(), updated);
                Some(ReplicaChange::SetFlags {
                    handle: local.handle.clone(),
                    flags: merged,
                })
            }
        }
    }

    /// Records a per-item event for the report.
    fn emit(&mut self, event: ReplicaEvent) {
        self.report.events.push(event);
    }

    fn drop(&mut self, handle: &ReplicaHandle) {
        self.writes.push(ReplicaWriteOp::DropPlacement {
            collection: self.collection.clone(),
            handle: handle.clone(),
        });
    }

    fn pull_add(&mut self, item: &ReplicaRemoteItem) {
        let placement = ReplicaPlacement {
            collection: self.collection.clone(),
            handle: item.handle.clone(),
            link_id: None,
            object: None,
            level: ReplicaLevel::Probed,
            meta: None,
            sort_key: ReplicaSortKey::default(),
            flags: item.flags.clone(),
            status: ReplicaStatus::Clean,
            conflict_revision: None,
            base: Some(ReplicaBase {
                flags: item.flags.clone(),
                revision: item.revision.clone(),
                object: None,
            }),
            origin: None,
        };
        self.writes.push(ReplicaWriteOp::UpsertPlacement(placement));
    }

    /// Pulls a remote content change: the stale local body is dropped and
    /// the base adopts the new revision. The level falls back to probed
    /// (the cached summary describes the old revision; it is kept as a
    /// display fallback until a meta upgrade refetches it). Flags and
    /// status are left for the flag reconciliation to settle.
    fn pull_content(
        &mut self,
        local: &ReplicaPlacement,
        item: &ReplicaRemoteItem,
    ) -> ReplicaPlacement {
        let mut updated = local.clone();
        updated.object = None;
        updated.level = ReplicaLevel::Probed;
        if let Some(base) = &mut updated.base {
            base.revision = item.revision.clone();
            base.object = None;
        }

        self.writes
            .push(ReplicaWriteOp::UpsertPlacement(updated.clone()));
        self.report.refreshed += 1;
        self.emit(ReplicaEvent::ContentChanged(local.handle.clone()));
        updated
    }

    /// Adopts `flags` as both the current and the base flag set. An
    /// unresolved content conflict keeps its status (only the flag axis
    /// converged), everything else lands clean.
    fn pull_flags(&mut self, local: &ReplicaPlacement, flags: &ReplicaFlags) {
        let mut updated = local.clone();
        updated.flags = flags.clone();
        if updated.status != ReplicaStatus::Conflict {
            updated.status = ReplicaStatus::Clean;
        }
        updated.base = Some(ReplicaBase {
            flags: flags.clone(),
            revision: local.base.as_ref().and_then(|b| b.revision.clone()),
            object: local.base.as_ref().and_then(|b| b.object.clone()),
        });
        self.writes.push(ReplicaWriteOp::UpsertPlacement(updated));
    }

    /// Rebases the flag axis onto `flags`, keeping the current flag set.
    /// An unresolved content conflict keeps its status (its resolution
    /// belongs to an edit), everything else lands clean.
    fn rebase(&mut self, local: &ReplicaPlacement, flags: &ReplicaFlags) {
        let mut updated = local.clone();
        if updated.status != ReplicaStatus::Conflict {
            updated.status = ReplicaStatus::Clean;
        }
        updated.base = Some(ReplicaBase {
            flags: flags.clone(),
            revision: local.base.as_ref().and_then(|b| b.revision.clone()),
            object: local.base.as_ref().and_then(|b| b.object.clone()),
        });
        self.writes.push(ReplicaWriteOp::UpsertPlacement(updated));
    }

    /// Rebases an accepted content push: the base pins the pushed body and
    /// the revision the remote reported. The base flags are left as they
    /// were, so a flag edit that rode along with the content edit stays
    /// derivable and pushes on the next sync.
    fn rebase_content(&mut self, local: &ReplicaPlacement, revision: Option<String>) {
        let base_flags = local
            .base
            .as_ref()
            .map(|b| b.flags.clone())
            .unwrap_or_default();

        let mut updated = local.clone();
        // NOTE: a tombstone stays a tombstone: its edit pushed ahead of
        // the pending move, which derives on the next sync.
        updated.status = if local.status == ReplicaStatus::Tombstone {
            ReplicaStatus::Tombstone
        } else if local.flags == base_flags {
            ReplicaStatus::Clean
        } else {
            ReplicaStatus::Dirty
        };
        updated.base = Some(ReplicaBase {
            flags: base_flags,
            revision,
            object: local.object.clone(),
        });
        self.writes.push(ReplicaWriteOp::UpsertPlacement(updated));
    }

    /// Rekeys an accepted create: drops the provisional placeholder and
    /// upserts the same placement under the server-assigned `handle`, clean
    /// and based, so the next enumerate reconciles it as already in sync.
    /// The base records the revision the push reported and pins the pushed
    /// body: the remote now holds exactly that content.
    fn rekey_create(
        &mut self,
        placeholder: ReplicaPlacement,
        handle: ReplicaHandle,
        revision: Option<String>,
    ) {
        self.drop(&placeholder.handle);
        let mut placed = placeholder;
        placed.handle = handle;
        placed.status = ReplicaStatus::Clean;
        placed.origin = None;
        placed.base = Some(ReplicaBase {
            flags: placed.flags.clone(),
            revision,
            object: placed.object.clone(),
        });
        self.writes.push(ReplicaWriteOp::UpsertPlacement(placed));
    }
}

impl ReplicaCoroutine for ReplicaSync {
    type Yield = ReplicaYield;
    type Return = Result<ReplicaSyncReport, ReplicaSyncError>;

    fn resume(
        &mut self,
        arg: Option<ReplicaArg>,
    ) -> ReplicaCoroutineState<Self::Yield, Self::Return> {
        match (&self.state, arg) {
            (State::Start, None) => {
                debug!("load local state from storage");
                self.state = State::PendingLoad;
                ReplicaCoroutineState::Yielded(ReplicaYield::WantsLoad(self.collection.clone()))
            }

            (State::PendingLoad, Some(ReplicaArg::Load(loaded))) => {
                self.local = loaded
                    .placements
                    .into_iter()
                    .map(|p| (p.handle.clone(), p))
                    .collect();
                // NOTE: a full sync ignores the checkpoint, so the enumerate
                // yields the whole remote and the merge reconciles the
                // entire spine.
                self.checkpoint = if self.opts.full {
                    None
                } else {
                    loaded.checkpoint
                };

                debug!("enumerate remote from checkpoint");
                trace!("loaded {} local items", self.local.len());
                self.state = State::PendingEnumerate;
                ReplicaCoroutineState::Yielded(ReplicaYield::WantsEnumerate {
                    collection: self.collection.clone(),
                    cursor: self.checkpoint.clone(),
                })
            }

            (State::PendingEnumerate, Some(ReplicaArg::Enumerate(snapshot))) => {
                trace!(
                    "enumerated {} items, {} vanished, complete={}",
                    snapshot.items.len(),
                    snapshot.vanished.len(),
                    snapshot.complete,
                );
                let pushes = self.reconcile(snapshot);

                if pushes.is_empty() {
                    debug!(
                        "reconciled pull-only: {} pulled, {} conflicts, write {} ops",
                        self.report.pulled,
                        self.report.conflicts,
                        self.writes.len(),
                    );
                    self.state = State::PendingWrite;
                    let writes = mem::take(&mut self.writes);
                    return ReplicaCoroutineState::Yielded(ReplicaYield::WantsWrite(writes));
                }

                debug!("push {} local changes to remote", pushes.len());
                trace!("changes: {pushes:?}");
                self.state = State::PendingPush;
                ReplicaCoroutineState::Yielded(ReplicaYield::WantsPush {
                    collection: self.collection.clone(),
                    changes: pushes,
                })
            }

            (State::PendingPush, Some(ReplicaArg::Push(results))) => {
                for result in &results {
                    match result.outcome {
                        // NOTE: the remote took the change: rebase a flag or
                        // content push clean, drop a confirmed delete, or
                        // rekey a confirmed create to its server-assigned
                        // handle, so the local state matches what the
                        // remote now holds.
                        ReplicaPushOutcome::Accepted => {
                            self.report.pushed += 1;
                            if let Some(placement) = self.pending_flag_pushes.remove(&result.handle)
                            {
                                let flags = placement.flags.clone();
                                self.rebase(&placement, &flags);
                                self.emit(ReplicaEvent::FlagsChanged(result.handle.clone()));
                            }
                            if let Some(placement) =
                                self.pending_content_pushes.remove(&result.handle)
                            {
                                self.rebase_content(&placement, result.revision.clone());
                                self.emit(ReplicaEvent::ContentChanged(result.handle.clone()));
                            }
                            if self.pending_drops.remove(&result.handle) {
                                self.drop(&result.handle);
                                self.emit(ReplicaEvent::Vanished(result.handle.clone()));
                            }
                            if let Some(placeholder) = self.pending_creates.remove(&result.handle) {
                                // NOTE: the created member's handle is the
                                // assigned one when the remote reports it,
                                // else the provisional.
                                let created = result
                                    .assigned
                                    .clone()
                                    .unwrap_or_else(|| result.handle.clone());
                                match result.assigned.clone() {
                                    // NOTE: remote returned the new handle:
                                    // rekey.
                                    Some(assigned) => self.rekey_create(
                                        placeholder,
                                        assigned,
                                        result.revision.clone(),
                                    ),
                                    // NOTE: no assigned handle (no UIDPLUS):
                                    // the copy landed, so drop the
                                    // placeholder; the next enumerate re-adds
                                    // it by its real handle and links the
                                    // body by link id.
                                    None => self.drop(&placeholder.handle),
                                }
                                self.emit(ReplicaEvent::Created(created));
                            }
                        }
                        // NOTE: the remote refused it: leave the dirty
                        // placement, tombstone or placeholder untouched so
                        // the next sync retries the push.
                        ReplicaPushOutcome::Rejected => self.report.rejected += 1,
                    }
                }
                // NOTE: any handle the push never reported on stays pending
                // too.
                self.pending_flag_pushes.clear();
                self.pending_content_pushes.clear();
                self.pending_drops.clear();
                self.pending_creates.clear();

                debug!("pushed, write {} storage ops", self.writes.len());
                trace!("push results: {results:?}");
                self.state = State::PendingWrite;
                let writes = mem::take(&mut self.writes);
                ReplicaCoroutineState::Yielded(ReplicaYield::WantsWrite(writes))
            }

            (State::PendingWrite, Some(ReplicaArg::Write)) => {
                debug!(
                    "sync done: {} pulled, {} pushed, {} refreshed, {} conflicts, {} rejected",
                    self.report.pulled,
                    self.report.pushed,
                    self.report.refreshed,
                    self.report.conflicts,
                    self.report.rejected,
                );
                ReplicaCoroutineState::Complete(Ok(mem::take(&mut self.report)))
            }

            (_, Some(_)) => ReplicaCoroutineState::Complete(Err(ReplicaSyncError::UnexpectedArg)),
            (_, None) => ReplicaCoroutineState::Complete(Err(ReplicaSyncError::MissingArg)),
        }
    }
}

/// What the content axis decided for a placement present on both sides.
enum ContentOutcome {
    /// No content signal on either side: the flag merge runs on the
    /// placement as loaded.
    Untouched,
    /// The placement was rewritten (a remote content pull, a fresh
    /// conflict mark, or conflict tracking): the flag merge runs on the
    /// rewritten copy, never on the stale loaded one.
    Rewritten(ReplicaPlacement),
    /// The local content won: the change to push.
    Push(ReplicaChange),
}

enum State {
    Start,
    PendingLoad,
    PendingEnumerate,
    PendingPush,
    PendingWrite,
}

#[cfg(test)]
mod tests {
    use alloc::{vec, vec::Vec};

    use crate::{
        object::ReplicaHash,
        placement::{ReplicaLinkId, ReplicaOrigin},
        remote::{ReplicaPushOutcome, ReplicaPushResult, ReplicaRemoteSnapshot},
        storage::ReplicaLoaded,
        sync::*,
    };

    /// A pending create staged in "inbox", its body sourced from "sent".
    fn created(handle: &str) -> ReplicaPlacement {
        ReplicaPlacement {
            sort_key: Default::default(),
            collection: "inbox".into(),
            handle: ReplicaHandle::from(handle),
            link_id: None,
            object: None,
            level: ReplicaLevel::Probed,
            meta: None,
            flags: ReplicaFlags::default(),
            status: ReplicaStatus::Created,
            conflict_revision: None,
            base: None,
            origin: Some(ReplicaOrigin {
                collection: "sent".into(),
                handle: ReplicaHandle::from("9"),
            }),
        }
    }

    fn synced(handle: &str, flags: &[&str]) -> ReplicaPlacement {
        ReplicaPlacement {
            sort_key: Default::default(),
            collection: "inbox".into(),
            handle: ReplicaHandle::from(handle),
            link_id: Some(ReplicaLinkId::from(handle)),
            object: None,
            level: ReplicaLevel::Probed,
            meta: None,
            flags: ReplicaFlags::from_iter(flags.iter().copied()),
            status: ReplicaStatus::Clean,
            conflict_revision: None,
            base: Some(ReplicaBase {
                flags: ReplicaFlags::from_iter(flags.iter().copied()),
                revision: None,
                object: None,
            }),
            origin: None,
        }
    }

    fn remote(handle: &str, flags: &[&str]) -> ReplicaRemoteItem {
        ReplicaRemoteItem {
            handle: ReplicaHandle::from(handle),
            flags: ReplicaFlags::from_iter(flags.iter().copied()),
            revision: None,
        }
    }

    fn full(items: Vec<ReplicaRemoteItem>) -> ReplicaRemoteSnapshot {
        ReplicaRemoteSnapshot {
            items,
            vanished: Vec::new(),
            complete: true,
            checkpoint: ReplicaCheckpoint(b"c1".to_vec()),
        }
    }

    fn delta(items: Vec<ReplicaRemoteItem>, vanished: Vec<ReplicaHandle>) -> ReplicaRemoteSnapshot {
        ReplicaRemoteSnapshot {
            items,
            vanished,
            complete: false,
            checkpoint: ReplicaCheckpoint(b"c1".to_vec()),
        }
    }

    fn run(
        sync: &mut ReplicaSync,
        local: Vec<ReplicaPlacement>,
        items: Vec<ReplicaRemoteItem>,
    ) -> (
        Option<Vec<ReplicaChange>>,
        Vec<ReplicaWriteOp>,
        ReplicaSyncReport,
    ) {
        run_snapshot(sync, local, full(items))
    }

    fn run_snapshot(
        sync: &mut ReplicaSync,
        local: Vec<ReplicaPlacement>,
        snapshot: ReplicaRemoteSnapshot,
    ) -> (
        Option<Vec<ReplicaChange>>,
        Vec<ReplicaWriteOp>,
        ReplicaSyncReport,
    ) {
        crate::testlog::init();
        let _ = sync.resume(None);
        let _ = sync.resume(Some(ReplicaArg::Load(ReplicaLoaded {
            placements: local,
            checkpoint: None,
        })));

        let mut pushes = None;
        let writes = match sync.resume(Some(ReplicaArg::Enumerate(snapshot))) {
            ReplicaCoroutineState::Yielded(ReplicaYield::WantsPush { changes, .. }) => {
                // the fake remote accepts everything, assigning nothing
                let results = changes
                    .iter()
                    .map(|change| {
                        let handle = match change {
                            ReplicaChange::Add { handle, .. } => handle,
                            ReplicaChange::Remove { handle, .. } => handle,
                            ReplicaChange::SetFlags { handle, .. } => handle,
                            ReplicaChange::Update { handle, .. } => handle,
                        };
                        ReplicaPushResult {
                            handle: handle.clone(),
                            outcome: ReplicaPushOutcome::Accepted,
                            assigned: None,
                            revision: None,
                        }
                    })
                    .collect();
                pushes = Some(changes);
                match sync.resume(Some(ReplicaArg::Push(results))) {
                    ReplicaCoroutineState::Yielded(ReplicaYield::WantsWrite(w)) => w,
                    state => panic!("expected WantsWrite, got {state:?}"),
                }
            }
            ReplicaCoroutineState::Yielded(ReplicaYield::WantsWrite(w)) => w,
            state => panic!("expected push or write, got {state:?}"),
        };

        let report = match sync.resume(Some(ReplicaArg::Write)) {
            ReplicaCoroutineState::Complete(Ok(report)) => report,
            state => panic!("expected Complete(Ok), got {state:?}"),
        };

        (pushes, writes, report)
    }

    #[test]
    fn remote_add_pulls_probed() {
        crate::testlog::init();
        let mut sync = ReplicaSync::new("inbox", ReplicaSyncOptions::default());
        let (pushes, writes, report) = run(&mut sync, vec![], vec![remote("1", &["seen"])]);

        assert!(pushes.is_none());
        assert_eq!(report.pulled, 1);
        let ReplicaWriteOp::UpsertPlacement(p) = &writes[0] else {
            panic!("expected UpsertPlacement, got {:?}", writes[0]);
        };
        assert_eq!(p.level, ReplicaLevel::Probed);
        assert!(p.flags.contains("seen"));
    }

    #[test]
    fn an_unknown_local_set_adopts_the_remote_one_and_pushes_nothing() {
        // A placement whose markers nobody has read yet holds no opinion, so
        // the sync pulls what the source reports rather than pushing an
        // absence over it (spec §13: `NULL` flags are unknown, not empty).
        let mut local = synced("1", &[]);
        local.flags = ReplicaFlags::Unknown;

        let mut sync = ReplicaSync::new("inbox", ReplicaSyncOptions::default());
        let (pushes, writes, report) = run(&mut sync, vec![local], vec![remote("1", &["seen"])]);

        assert!(pushes.is_none(), "nothing to push from an unknown set");
        assert_eq!(report.pulled, 1);
        let ReplicaWriteOp::UpsertPlacement(p) = &writes[0] else {
            panic!("expected UpsertPlacement, got {:?}", writes[0]);
        };
        assert!(p.flags.contains("seen"));
    }

    #[test]
    fn local_flag_change_pushes() {
        let mut local = synced("1", &[]);
        local.flags = ReplicaFlags::from_iter(["seen"]);
        local.status = ReplicaStatus::Dirty;

        let mut sync = ReplicaSync::new("inbox", ReplicaSyncOptions::default());
        let (pushes, _writes, report) = run(&mut sync, vec![local], vec![remote("1", &[])]);

        let pushes = pushes.expect("a push");
        assert!(matches!(pushes[0], ReplicaChange::SetFlags { .. }));
        assert_eq!(report.pushed, 1);
    }

    /// Drives a sync through its push, feeding back the given push results,
    /// and returns the storage writes the engine then stages and the report.
    fn drive_push(
        sync: &mut ReplicaSync,
        local: Vec<ReplicaPlacement>,
        items: Vec<ReplicaRemoteItem>,
        results: Vec<ReplicaPushResult>,
    ) -> (Vec<ReplicaWriteOp>, ReplicaSyncReport) {
        crate::testlog::init();
        let _ = sync.resume(None);
        let _ = sync.resume(Some(ReplicaArg::Load(ReplicaLoaded {
            placements: local,
            checkpoint: None,
        })));
        match sync.resume(Some(ReplicaArg::Enumerate(full(items)))) {
            ReplicaCoroutineState::Yielded(ReplicaYield::WantsPush { .. }) => {}
            state => panic!("expected WantsPush, got {state:?}"),
        }
        let writes = match sync.resume(Some(ReplicaArg::Push(results))) {
            ReplicaCoroutineState::Yielded(ReplicaYield::WantsWrite(writes)) => writes,
            state => panic!("expected WantsWrite, got {state:?}"),
        };
        let report = match sync.resume(Some(ReplicaArg::Write)) {
            ReplicaCoroutineState::Complete(Ok(report)) => report,
            state => panic!("expected Complete(Ok), got {state:?}"),
        };
        (writes, report)
    }

    /// Finds the placement an UpsertPlacement op writes for `handle`, if any.
    fn upserted<'a>(writes: &'a [ReplicaWriteOp], handle: &str) -> Option<&'a ReplicaPlacement> {
        writes.iter().find_map(|w| match w {
            ReplicaWriteOp::UpsertPlacement(p) if p.handle.as_str() == handle => Some(p),
            _ => None,
        })
    }

    #[test]
    fn rejected_flag_push_keeps_dirty() {
        let mut local = synced("1", &[]);
        local.flags = ReplicaFlags::from_iter(["flagged"]);
        local.status = ReplicaStatus::Dirty;

        let mut sync = ReplicaSync::new("inbox", ReplicaSyncOptions::default());
        let results = vec![ReplicaPushResult {
            handle: ReplicaHandle::from("1"),
            outcome: ReplicaPushOutcome::Rejected,
            assigned: None,
            revision: None,
        }];
        let (writes, report) = drive_push(&mut sync, vec![local], vec![remote("1", &[])], results);

        assert!(
            upserted(&writes, "1").is_none(),
            "a rejected flag push must not rebase the placement: {writes:?}",
        );
        assert_eq!(report.rejected, 1);
    }

    #[test]
    fn accepted_flag_push_rebases_clean() {
        let mut local = synced("1", &[]);
        local.flags = ReplicaFlags::from_iter(["flagged"]);
        local.status = ReplicaStatus::Dirty;

        let mut sync = ReplicaSync::new("inbox", ReplicaSyncOptions::default());
        let results = vec![ReplicaPushResult {
            handle: ReplicaHandle::from("1"),
            outcome: ReplicaPushOutcome::Accepted,
            assigned: None,
            revision: None,
        }];
        let (writes, _report) = drive_push(&mut sync, vec![local], vec![remote("1", &[])], results);

        let rebased = upserted(&writes, "1").expect("an accepted flag push rebases the placement");
        assert_eq!(rebased.status, ReplicaStatus::Clean);
        assert!(
            rebased
                .base
                .as_ref()
                .expect("a base")
                .flags
                .contains("flagged")
        );
    }

    #[test]
    fn partial_push_accepts_one_rejects_other() {
        let mut one = synced("1", &[]);
        one.flags = ReplicaFlags::from_iter(["flagged"]);
        one.status = ReplicaStatus::Dirty;
        let mut two = synced("2", &[]);
        two.flags = ReplicaFlags::from_iter(["flagged"]);
        two.status = ReplicaStatus::Dirty;

        let mut sync = ReplicaSync::new("inbox", ReplicaSyncOptions::default());
        let results = vec![
            ReplicaPushResult {
                handle: ReplicaHandle::from("1"),
                outcome: ReplicaPushOutcome::Accepted,
                assigned: None,
                revision: None,
            },
            ReplicaPushResult {
                handle: ReplicaHandle::from("2"),
                outcome: ReplicaPushOutcome::Rejected,
                assigned: None,
                revision: None,
            },
        ];
        let (writes, report) = drive_push(
            &mut sync,
            vec![one, two],
            vec![remote("1", &[]), remote("2", &[])],
            results,
        );

        // The accepted handle rebases clean; the rejected one is left dirty
        // (no write), so the next sync retries it.
        assert_eq!(
            upserted(&writes, "1").expect("accepted rebases").status,
            ReplicaStatus::Clean,
        );
        assert!(
            upserted(&writes, "2").is_none(),
            "rejected handle must stay dirty: {writes:?}",
        );
        assert_eq!(report.pushed, 1, "only the accepted change counts");
        assert_eq!(report.rejected, 1);
    }

    #[test]
    fn rejected_push_retries_on_next_sync() {
        let mut local = synced("1", &[]);
        local.flags = ReplicaFlags::from_iter(["flagged"]);
        local.status = ReplicaStatus::Dirty;

        // First sync: the remote rejects the push, so nothing is rebased and
        // the placement stays dirty in storage.
        let mut first = ReplicaSync::new("inbox", ReplicaSyncOptions::default());
        let results = vec![ReplicaPushResult {
            handle: ReplicaHandle::from("1"),
            outcome: ReplicaPushOutcome::Rejected,
            assigned: None,
            revision: None,
        }];
        let (writes, _report) = drive_push(
            &mut first,
            vec![local.clone()],
            vec![remote("1", &[])],
            results,
        );
        assert!(upserted(&writes, "1").is_none(), "rejected push left dirty");

        // Second sync over the still-dirty placement (the remote never took
        // it, so its flags are unchanged upstream): the push is attempted
        // again, proving a rejection is not silently dropped.
        let mut second = ReplicaSync::new("inbox", ReplicaSyncOptions::default());
        let (pushes, _writes, report) = run(&mut second, vec![local], vec![remote("1", &[])]);
        let pushes = pushes.expect("the dirty change is pushed again");
        assert!(matches!(pushes[0], ReplicaChange::SetFlags { .. }));
        assert_eq!(report.pushed, 1);
    }

    #[test]
    fn remote_flag_change_pulls() {
        let local = synced("1", &[]);
        let mut sync = ReplicaSync::new("inbox", ReplicaSyncOptions::default());
        let (pushes, writes, report) = run(&mut sync, vec![local], vec![remote("1", &["seen"])]);

        assert!(pushes.is_none());
        assert_eq!(report.pulled, 1);
        let ReplicaWriteOp::UpsertPlacement(p) = &writes[0] else {
            panic!("expected UpsertPlacement, got {:?}", writes[0]);
        };
        assert!(p.flags.contains("seen"));
        assert_eq!(p.status, ReplicaStatus::Clean);
    }

    #[test]
    fn divergent_flags_merge_element_wise() {
        // Local added "flagged", remote added "seen", from an empty base:
        // each side wins its own flag; the merged union is pushed and the
        // remote-won flag folded in locally, with no conflict.
        let mut local = synced("1", &[]);
        local.flags = ReplicaFlags::from_iter(["flagged"]);
        local.status = ReplicaStatus::Dirty;

        let mut sync = ReplicaSync::new("inbox", ReplicaSyncOptions::default());
        let (pushes, writes, report) = run(&mut sync, vec![local], vec![remote("1", &["seen"])]);

        match &pushes.expect("a push")[0] {
            ReplicaChange::SetFlags { flags, .. } => {
                assert!(flags.contains("flagged") && flags.contains("seen"));
            }
            other => panic!("expected a SetFlags push, got {other:?}"),
        }
        assert_eq!(report.conflicts, 0);
        assert_eq!(report.pulled, 1, "the remote-won flag is folded in");

        // last write chain entry: the accepted push rebased the merge clean
        let rebased = writes
            .iter()
            .rev()
            .find_map(|w| match w {
                ReplicaWriteOp::UpsertPlacement(p) if p.handle.as_str() == "1" => Some(p),
                _ => None,
            })
            .expect("a rebased placement");
        assert_eq!(rebased.status, ReplicaStatus::Clean);
        assert!(rebased.flags.contains("flagged") && rebased.flags.contains("seen"));
        let base = rebased.base.as_ref().expect("a base");
        assert!(base.flags.contains("flagged") && base.flags.contains("seen"));
    }

    #[test]
    fn flag_removal_merges_against_concurrent_addition() {
        // Local removed the base flag "seen" while the remote added
        // "important": the local removal and the remote addition both win.
        let mut local = synced("1", &["seen"]);
        local.flags = ReplicaFlags::default();
        local.status = ReplicaStatus::Dirty;

        let mut sync = ReplicaSync::new("inbox", ReplicaSyncOptions::default());
        let (pushes, _writes, report) = run(
            &mut sync,
            vec![local],
            vec![remote("1", &["seen", "important"])],
        );

        match &pushes.expect("a push")[0] {
            ReplicaChange::SetFlags { flags, .. } => {
                assert!(flags.contains("important"), "the remote addition wins");
                assert!(!flags.contains("seen"), "the local removal wins");
            }
            other => panic!("expected a SetFlags push, got {other:?}"),
        }
        assert_eq!(report.conflicts, 0);
    }

    #[test]
    fn read_only_keeps_local_dirty() {
        let mut local = synced("1", &[]);
        local.flags = ReplicaFlags::from_iter(["seen"]);
        local.status = ReplicaStatus::Dirty;

        let opts = ReplicaSyncOptions {
            push: false,
            rights: ReplicaPushRights::all(),
            conflict: ReplicaConflictPolicy::Manual,
            full: false,
        };
        let mut sync = ReplicaSync::new("inbox", opts);
        let (pushes, _writes, report) = run(&mut sync, vec![local], vec![remote("1", &[])]);

        assert!(pushes.is_none(), "read-only source must not push");
        assert_eq!(report.pushed, 0);
    }

    #[test]
    fn delta_vanished_drops() {
        let local = synced("1", &["seen"]);
        let mut sync = ReplicaSync::new("inbox", ReplicaSyncOptions::default());
        let snapshot = delta(vec![], vec![ReplicaHandle::from("1")]);
        let (pushes, writes, report) = run_snapshot(&mut sync, vec![local], snapshot);

        assert!(pushes.is_none());
        assert_eq!(report.pulled, 1);
        assert!(
            matches!(&writes[0], ReplicaWriteOp::DropPlacement { handle, .. } if handle.as_str() == "1"),
            "vanished placement dropped, got {:?}",
            writes[0],
        );
    }

    #[test]
    fn delta_pull_add() {
        let mut sync = ReplicaSync::new("inbox", ReplicaSyncOptions::default());
        let snapshot = delta(vec![remote("9", &["seen"])], vec![]);
        let (pushes, writes, report) = run_snapshot(&mut sync, vec![], snapshot);

        assert!(pushes.is_none());
        assert_eq!(report.pulled, 1);
        let ReplicaWriteOp::UpsertPlacement(p) = &writes[0] else {
            panic!("expected UpsertPlacement, got {:?}", writes[0]);
        };
        assert_eq!(p.handle.as_str(), "9");
        assert_eq!(p.level, ReplicaLevel::Probed);
    }

    #[test]
    fn delta_leaves_unlisted_untouched() {
        let one = synced("1", &[]);
        let two = synced("2", &[]);
        let mut sync = ReplicaSync::new("inbox", ReplicaSyncOptions::default());
        // only "2" changed upstream; "1" is unlisted and must not be touched
        let snapshot = delta(vec![remote("2", &["seen"])], vec![]);
        let (pushes, writes, report) = run_snapshot(&mut sync, vec![one, two], snapshot);

        assert!(pushes.is_none());
        assert_eq!(report.pulled, 1);
        assert_eq!(writes.len(), 2, "only the changed placement and checkpoint");
        let ReplicaWriteOp::UpsertPlacement(p) = &writes[0] else {
            panic!("expected UpsertPlacement, got {:?}", writes[0]);
        };
        assert_eq!(p.handle.as_str(), "2");
        assert!(p.flags.contains("seen"));
    }

    #[test]
    fn delta_pushes_unlisted_local_dirty() {
        let mut local = synced("1", &[]);
        local.flags = ReplicaFlags::from_iter(["seen"]);
        local.status = ReplicaStatus::Dirty;

        let mut sync = ReplicaSync::new("inbox", ReplicaSyncOptions::default());
        // the dirty handle is not in the delta, but its pending push must
        // still be derived against its own base
        let snapshot = delta(vec![], vec![]);
        let (pushes, _writes, report) = run_snapshot(&mut sync, vec![local], snapshot);

        let pushes = pushes.expect("a push");
        assert!(matches!(pushes[0], ReplicaChange::SetFlags { .. }));
        assert_eq!(report.pushed, 1);
    }

    #[test]
    fn unchanged_flags_is_noop() {
        // local == base == remote: nothing to pull or push, no placement write.
        let local = synced("1", &["seen"]);
        let mut sync = ReplicaSync::new("inbox", ReplicaSyncOptions::default());
        let (pushes, writes, report) = run(&mut sync, vec![local], vec![remote("1", &["seen"])]);

        assert!(pushes.is_none());
        assert_eq!(report, ReplicaSyncReport::default(), "a no-op sync");
        assert!(
            upserted(&writes, "1").is_none(),
            "an unchanged placement is not rewritten: {writes:?}",
        );
    }

    #[test]
    fn concurrent_same_flags_rebases_without_push() {
        // Both sides moved to the same flags from a shared base: converge
        // clean, no push and no conflict.
        let mut local = synced("1", &[]);
        local.flags = ReplicaFlags::from_iter(["flagged"]);
        local.status = ReplicaStatus::Dirty;

        let mut sync = ReplicaSync::new("inbox", ReplicaSyncOptions::default());
        let (pushes, writes, report) = run(&mut sync, vec![local], vec![remote("1", &["flagged"])]);

        assert!(pushes.is_none(), "no push when both reached the same flags");
        assert_eq!(report.conflicts, 0);
        let rebased = upserted(&writes, "1").expect("a converging rebase");
        assert_eq!(rebased.status, ReplicaStatus::Clean);
        assert!(
            rebased
                .base
                .as_ref()
                .expect("a base")
                .flags
                .contains("flagged")
        );
    }

    #[test]
    fn no_base_present_converges_on_remote() {
        // Present on both but never based (no base to diff against): the
        // remote wins and the placement is rebased onto it.
        let mut local = synced("1", &["flagged"]);
        local.base = None;

        let mut sync = ReplicaSync::new("inbox", ReplicaSyncOptions::default());
        let (pushes, writes, report) = run(&mut sync, vec![local], vec![remote("1", &["seen"])]);

        assert!(pushes.is_none());
        assert_eq!(report.pulled, 1);
        let pulled = upserted(&writes, "1").expect("a converged placement");
        assert_eq!(pulled.status, ReplicaStatus::Clean);
        assert!(pulled.flags.contains("seen"));
        assert!(!pulled.flags.contains("flagged"), "remote flags win");
    }

    #[test]
    fn flag_pull_on_a_conflicted_placement_keeps_the_conflict() {
        // A remote flag change on a content-conflicted placement pulls the
        // flags but must not launder the conflict away: the status stays
        // Conflict and the staged local edit survives, so the next sync
        // never mistakes the placement for clean and drops the edit.
        let mut placement = edited("1");
        placement.status = ReplicaStatus::Conflict;
        placement.conflict_revision = Some("r2".into());

        let mut sync = ReplicaSync::new("inbox", ReplicaSyncOptions::default());
        let mut item = remote_rev("1", "r2");
        item.flags = ReplicaFlags::from_iter(["seen"]);
        let (pushes, writes, report) = run(&mut sync, vec![placement], vec![item]);

        assert!(pushes.is_none());
        assert_eq!(report.pulled, 1);
        let pulled = upserted(&writes, "1").expect("a flag pull");
        assert_eq!(
            pulled.status,
            ReplicaStatus::Conflict,
            "the conflict survives"
        );
        assert!(pulled.flags.contains("seen"));
        assert_eq!(
            pulled.object,
            Some(ReplicaHash::from("h2")),
            "the edit survives"
        );
    }

    #[test]
    fn read_only_still_pulls_remote_changes() {
        // push=false blocks the push direction only; remote-won changes are
        // still pulled.
        let local = synced("1", &[]);
        let opts = ReplicaSyncOptions {
            push: false,
            rights: ReplicaPushRights::all(),
            conflict: ReplicaConflictPolicy::Manual,
            full: false,
        };
        let mut sync = ReplicaSync::new("inbox", opts);
        let (pushes, writes, report) = run(&mut sync, vec![local], vec![remote("1", &["seen"])]);

        assert!(pushes.is_none());
        assert_eq!(report.pulled, 1);
        assert!(
            upserted(&writes, "1")
                .expect("a pull")
                .flags
                .contains("seen")
        );
    }

    #[test]
    fn accepted_delete_drops_tombstone() {
        // A tombstone present on both sides pushes a Remove; once the remote
        // accepts it, the placement is dropped.
        let mut local = synced("1", &["seen"]);
        local.status = ReplicaStatus::Tombstone;

        let mut sync = ReplicaSync::new("inbox", ReplicaSyncOptions::default());
        let results = vec![ReplicaPushResult {
            handle: ReplicaHandle::from("1"),
            outcome: ReplicaPushOutcome::Accepted,
            assigned: None,
            revision: None,
        }];
        let (writes, report) = drive_push(
            &mut sync,
            vec![local],
            vec![remote("1", &["seen"])],
            results,
        );

        assert_eq!(report.pushed, 1);
        assert!(
            writes.iter().any(
                |w| matches!(w, ReplicaWriteOp::DropPlacement { handle, .. } if handle.as_str() == "1")
            ),
            "an accepted delete drops the tombstone: {writes:?}",
        );
    }

    #[test]
    fn rejected_delete_keeps_tombstone() {
        // A delete the remote refuses (e.g. no trash to move into) must keep
        // the tombstone, not drop a message the server still has.
        let mut local = synced("1", &["seen"]);
        local.status = ReplicaStatus::Tombstone;

        let mut sync = ReplicaSync::new("inbox", ReplicaSyncOptions::default());
        let results = vec![ReplicaPushResult {
            handle: ReplicaHandle::from("1"),
            outcome: ReplicaPushOutcome::Rejected,
            assigned: None,
            revision: None,
        }];
        let (writes, report) = drive_push(
            &mut sync,
            vec![local],
            vec![remote("1", &["seen"])],
            results,
        );

        assert_eq!(report.rejected, 1);
        assert!(
            !writes.iter().any(
                |w| matches!(w, ReplicaWriteOp::DropPlacement { handle, .. } if handle.as_str() == "1")
            ),
            "a rejected delete must not drop the tombstone: {writes:?}",
        );
    }

    #[test]
    fn local_delete_gone_remote_just_drops() {
        // A tombstone whose message already vanished upstream needs no push.
        let mut local = synced("1", &["seen"]);
        local.status = ReplicaStatus::Tombstone;

        let mut sync = ReplicaSync::new("inbox", ReplicaSyncOptions::default());
        let (pushes, writes, report) = run(&mut sync, vec![local], vec![]);

        assert!(pushes.is_none());
        assert_eq!(report.pushed, 0);
        assert!(
            writes.iter().any(
                |w| matches!(w, ReplicaWriteOp::DropPlacement { handle, .. } if handle.as_str() == "1")
            ),
            "the tombstone is dropped without a push: {writes:?}",
        );
    }

    #[test]
    fn remote_delete_in_full_drops() {
        // A based placement absent from a complete snapshot was deleted
        // upstream: drop it and count a pull.
        let local = synced("1", &["seen"]);
        let mut sync = ReplicaSync::new("inbox", ReplicaSyncOptions::default());
        let (pushes, writes, report) = run(&mut sync, vec![local], vec![]);

        assert!(pushes.is_none());
        assert_eq!(report.pulled, 1);
        assert!(
            writes.iter().any(
                |w| matches!(w, ReplicaWriteOp::DropPlacement { handle, .. } if handle.as_str() == "1")
            ),
            "the vanished placement is dropped: {writes:?}",
        );
    }

    #[test]
    fn offline_created_item_left_for_create_path() {
        // Present locally, never based, not upstream: an offline-created item
        // the sync leaves untouched (its upload belongs to the create path).
        let mut local = synced("1", &["flagged"]);
        local.base = None;
        local.status = ReplicaStatus::Dirty;

        let mut sync = ReplicaSync::new("inbox", ReplicaSyncOptions::default());
        let (pushes, writes, report) = run(&mut sync, vec![local], vec![]);

        assert!(pushes.is_none());
        assert_eq!(report, ReplicaSyncReport::default());
        assert!(
            upserted(&writes, "1").is_none()
                && !writes.iter().any(
                    |w| matches!(w, ReplicaWriteOp::DropPlacement { handle, .. } if handle.as_str() == "1")
                ),
            "an offline-created item is neither rewritten nor dropped: {writes:?}",
        );
    }

    #[test]
    fn created_placement_pushes_add() {
        // A Created placement (no base, not upstream) pushes an Add carrying
        // its origin, so the remote can copy rather than re-upload, and its
        // flag set, so an append creates the member with the right flags.
        let mut local = created("tmp-1");
        local.flags = ReplicaFlags::from_iter(["seen"]);
        let mut sync = ReplicaSync::new("inbox", ReplicaSyncOptions::default());
        let (pushes, _writes, report) = run(&mut sync, vec![local], vec![]);

        match &pushes.expect("a push")[0] {
            ReplicaChange::Add { origin, flags, .. } => {
                assert!(origin.is_some());
                assert!(flags.contains("seen"), "the flag set rides the add");
            }
            other => panic!("expected an Add push, got {other:?}"),
        }
        assert_eq!(report.pushed, 1);
    }

    #[test]
    fn accepted_create_rekeys_to_assigned() {
        // Once the add is accepted, the provisional placeholder is dropped
        // and the placement is rekeyed clean and based under the assigned
        // handle the server returned; the base records the pushed revision
        // and pins the pushed body.
        let mut local = created("tmp-1");
        local.object = Some(ReplicaHash::from("h-1"));
        let mut sync = ReplicaSync::new("inbox", ReplicaSyncOptions::default());
        let results = vec![ReplicaPushResult {
            handle: ReplicaHandle::from("tmp-1"),
            outcome: ReplicaPushOutcome::Accepted,
            assigned: Some(ReplicaHandle::from("42")),
            revision: Some("r1".into()),
        }];
        let (writes, _report) = drive_push(&mut sync, vec![local], vec![], results);

        assert!(
            writes.iter().any(
                |w| matches!(w, ReplicaWriteOp::DropPlacement { handle, .. } if handle.as_str() == "tmp-1")
            ),
            "the placeholder is dropped: {writes:?}",
        );
        let real =
            upserted(&writes, "42").expect("the placement is rekeyed to the assigned handle");
        assert_eq!(real.status, ReplicaStatus::Clean);
        assert!(real.origin.is_none());
        let base = real.base.as_ref().expect("a base");
        assert_eq!(base.revision.as_deref(), Some("r1"));
        assert_eq!(base.object, Some(ReplicaHash::from("h-1")));
    }

    #[test]
    fn rejected_create_keeps_placeholder() {
        // A refused add keeps the placeholder for the next retry: no drop and
        // no rekey.
        let local = created("tmp-1");
        let mut sync = ReplicaSync::new("inbox", ReplicaSyncOptions::default());
        let results = vec![ReplicaPushResult {
            handle: ReplicaHandle::from("tmp-1"),
            outcome: ReplicaPushOutcome::Rejected,
            assigned: None,
            revision: None,
        }];
        let (writes, report) = drive_push(&mut sync, vec![local], vec![], results);

        assert_eq!(report.rejected, 1);
        assert!(
            !writes
                .iter()
                .any(|w| matches!(w, ReplicaWriteOp::DropPlacement { .. })),
            "a rejected create must not drop the placeholder: {writes:?}",
        );
        assert!(upserted(&writes, "tmp-1").is_none());
    }

    #[test]
    fn move_pushes_remove_with_target() {
        // A tombstone carrying an origin is a move: it pushes a Remove naming
        // the destination, so the consumer issues a UID MOVE.
        let mut local = synced("1", &["seen"]);
        local.status = ReplicaStatus::Tombstone;
        local.origin = Some(ReplicaOrigin {
            collection: "archive".into(),
            handle: ReplicaHandle::from("1"),
        });

        let mut sync = ReplicaSync::new("inbox", ReplicaSyncOptions::default());
        let (pushes, _writes, report) = run(&mut sync, vec![local], vec![remote("1", &["seen"])]);

        match &pushes.expect("a push")[0] {
            ReplicaChange::Remove { to: Some(to), .. } => assert_eq!(to.as_str(), "archive"),
            other => panic!("expected a move Remove, got {other:?}"),
        }
        assert_eq!(report.pushed, 1);
    }

    #[test]
    fn accepted_create_without_assigned_drops_placeholder() {
        // A copy whose push is accepted with no assigned handle (no UIDPLUS)
        // drops the placeholder: the next enumerate re-adds the real handle.
        let local = created("tmp-1");
        let mut sync = ReplicaSync::new("inbox", ReplicaSyncOptions::default());
        let results = vec![ReplicaPushResult {
            handle: ReplicaHandle::from("tmp-1"),
            outcome: ReplicaPushOutcome::Accepted,
            assigned: None,
            revision: None,
        }];
        let (writes, _report) = drive_push(&mut sync, vec![local], vec![], results);

        assert!(
            writes.iter().any(
                |w| matches!(w, ReplicaWriteOp::DropPlacement { handle, .. } if handle.as_str() == "tmp-1")
            ),
            "the placeholder is dropped once the copy lands: {writes:?}",
        );
        assert!(upserted(&writes, "tmp-1").is_none());
    }

    #[test]
    fn full_sync_ignores_checkpoint() {
        // A full sync drops the stored checkpoint, so the enumerate is asked
        // for the whole remote (cursor None) rather than a delta.
        let mut sync = ReplicaSync::new(
            "inbox",
            ReplicaSyncOptions {
                push: true,
                rights: ReplicaPushRights::all(),
                conflict: ReplicaConflictPolicy::Manual,
                full: true,
            },
        );
        let _ = sync.resume(None);
        let loaded = ReplicaLoaded {
            placements: Vec::new(),
            checkpoint: Some(ReplicaCheckpoint(b"cp".to_vec())),
        };
        match sync.resume(Some(ReplicaArg::Load(loaded))) {
            ReplicaCoroutineState::Yielded(ReplicaYield::WantsEnumerate { cursor, .. }) => {
                assert!(cursor.is_none(), "a full sync must ignore the checkpoint");
            }
            state => panic!("expected WantsEnumerate, got {state:?}"),
        }
    }

    /// A synced placement carrying a staged content edit: the body points
    /// at "h2" while the base pins "h1" at revision "r1".
    fn edited(handle: &str) -> ReplicaPlacement {
        let mut placement = synced(handle, &[]);
        placement.status = ReplicaStatus::Dirty;
        placement.object = Some(ReplicaHash::from("h2"));
        placement.level = ReplicaLevel::Full;
        let base = placement.base.as_mut().expect("a base");
        base.revision = Some("r1".into());
        base.object = Some(ReplicaHash::from("h1"));
        placement
    }

    /// A remote item at the given content revision.
    fn remote_rev(handle: &str, revision: &str) -> ReplicaRemoteItem {
        let mut item = remote(handle, &[]);
        item.revision = Some(revision.into());
        item
    }

    #[test]
    fn local_content_edit_pushes_update_and_rebases() {
        // A staged edit against an unchanged remote pushes an Update gated
        // on the base revision; once accepted, the base pins the pushed
        // body and the revision the remote reported.
        let mut sync = ReplicaSync::new("inbox", ReplicaSyncOptions::default());
        let _ = sync.resume(None);
        let _ = sync.resume(Some(ReplicaArg::Load(ReplicaLoaded {
            placements: vec![edited("1")],
            checkpoint: None,
        })));

        let pushes = match sync.resume(Some(ReplicaArg::Enumerate(full(vec![remote_rev(
            "1", "r1",
        )])))) {
            ReplicaCoroutineState::Yielded(ReplicaYield::WantsPush { changes, .. }) => changes,
            state => panic!("expected WantsPush, got {state:?}"),
        };
        match &pushes[0] {
            ReplicaChange::Update {
                handle,
                object,
                if_match,
            } => {
                assert_eq!(handle.as_str(), "1");
                assert_eq!(object, &ReplicaHash::from("h2"));
                assert_eq!(if_match.as_deref(), Some("r1"));
            }
            other => panic!("expected an Update push, got {other:?}"),
        }

        let results = vec![ReplicaPushResult {
            handle: ReplicaHandle::from("1"),
            outcome: ReplicaPushOutcome::Accepted,
            assigned: None,
            revision: Some("r2".into()),
        }];
        let writes = match sync.resume(Some(ReplicaArg::Push(results))) {
            ReplicaCoroutineState::Yielded(ReplicaYield::WantsWrite(writes)) => writes,
            state => panic!("expected WantsWrite, got {state:?}"),
        };

        let rebased = upserted(&writes, "1").expect("a rebased placement");
        assert_eq!(rebased.status, ReplicaStatus::Clean);
        let base = rebased.base.as_ref().expect("a base");
        assert_eq!(base.revision.as_deref(), Some("r2"));
        assert_eq!(base.object, Some(ReplicaHash::from("h2")));
    }

    #[test]
    fn content_rebase_defers_a_riding_flag_edit() {
        // A placement both content-dirty and flag-dirty pushes the content;
        // the rebase keeps the base flags as they were and the placement
        // dirty, so the flag push derives on the next sync instead of
        // being silently marked synced.
        let mut placement = edited("1");
        placement.flags = ReplicaFlags::from_iter(["seen"]);

        let mut sync = ReplicaSync::new("inbox", ReplicaSyncOptions::default());
        let results = vec![ReplicaPushResult {
            handle: ReplicaHandle::from("1"),
            outcome: ReplicaPushOutcome::Accepted,
            assigned: None,
            revision: Some("r2".into()),
        }];
        let (writes, _report) = drive_push(
            &mut sync,
            vec![placement],
            vec![remote_rev("1", "r1")],
            results,
        );

        let rebased = upserted(&writes, "1").expect("a rebased placement");
        assert_eq!(
            rebased.status,
            ReplicaStatus::Dirty,
            "the flag edit stays pending"
        );
        let base = rebased.base.as_ref().expect("a base");
        assert!(!base.flags.contains("seen"), "base flags stay as synced");
        assert_eq!(base.object, Some(ReplicaHash::from("h2")));
    }

    #[test]
    fn remote_content_change_refreshes_the_stale_body() {
        // A remote edit of a clean placement drops the stale body and
        // rebases the revision; the next upgrade refetches on demand.
        let mut placement = synced("1", &[]);
        placement.object = Some(ReplicaHash::from("h1"));
        placement.level = ReplicaLevel::Full;
        let base = placement.base.as_mut().expect("a base");
        base.revision = Some("r1".into());
        base.object = Some(ReplicaHash::from("h1"));

        let mut sync = ReplicaSync::new("inbox", ReplicaSyncOptions::default());
        let (pushes, writes, report) = run(&mut sync, vec![placement], vec![remote_rev("1", "r2")]);

        assert!(pushes.is_none(), "a refresh pushes nothing");
        assert_eq!(report.refreshed, 1);
        let refreshed = upserted(&writes, "1").expect("a refreshed placement");
        assert_eq!(refreshed.object, None, "the stale body is dropped");
        assert_eq!(
            refreshed.level,
            ReplicaLevel::Probed,
            "the summary is stale too"
        );
        let base = refreshed.base.as_ref().expect("a base");
        assert_eq!(base.revision.as_deref(), Some("r2"));
        assert_eq!(base.object, None);
    }

    #[test]
    fn divergent_content_edits_conflict() {
        // Both sides edited: the placement is marked conflicted carrying
        // the observed remote revision, the local body survives, and
        // nothing is pushed or refreshed.
        let mut sync = ReplicaSync::new("inbox", ReplicaSyncOptions::default());
        let (pushes, writes, report) =
            run(&mut sync, vec![edited("1")], vec![remote_rev("1", "r2")]);

        assert!(pushes.is_none());
        assert_eq!(report.conflicts, 1);
        assert_eq!(report.refreshed, 0);
        let conflicted = upserted(&writes, "1").expect("a conflicted placement");
        assert_eq!(conflicted.status, ReplicaStatus::Conflict);
        assert_eq!(conflicted.conflict_revision.as_deref(), Some("r2"));
        assert_eq!(
            conflicted.object,
            Some(ReplicaHash::from("h2")),
            "the edit survives"
        );
    }

    #[test]
    fn base_less_body_present_on_both_conflicts() {
        // A create-collision, or a spoke whose content base was orphaned:
        // the placement carries a body but has no base, yet the remote
        // holds the same handle. With no shared ancestor there is nothing
        // to merge, so it is surfaced as a conflict carrying the remote
        // revision instead of converging on flags alone, which would
        // strand the two bodies apart and loop every sync. The consumer's
        // resolution then re-establishes a base, so the state self-heals.
        let mut placement = synced("1", &[]);
        placement.base = None;
        placement.object = Some(ReplicaHash::from("h1"));
        placement.level = ReplicaLevel::Full;

        let mut sync = ReplicaSync::new("inbox", ReplicaSyncOptions::default());
        let (pushes, writes, report) = run(&mut sync, vec![placement], vec![remote_rev("1", "r9")]);

        assert!(pushes.is_none(), "an unresolved conflict pushes nothing");
        assert_eq!(report.conflicts, 1);
        let conflicted = upserted(&writes, "1").expect("a conflicted placement");
        assert_eq!(conflicted.status, ReplicaStatus::Conflict);
        assert_eq!(conflicted.conflict_revision.as_deref(), Some("r9"));
        assert_eq!(
            conflicted.object,
            Some(ReplicaHash::from("h1")),
            "the body survives for the resolution"
        );
    }

    #[test]
    fn base_less_body_less_present_on_both_stays_flag_only() {
        // A never-based placement with no body carries no content signal,
        // so it must still converge on flags alone (the create-collision
        // rule only fires when a body is actually present on both sides).
        let mut placement = synced("1", &["seen"]);
        placement.base = None;

        let mut sync = ReplicaSync::new("inbox", ReplicaSyncOptions::default());
        let (pushes, writes, report) =
            run(&mut sync, vec![placement], vec![remote("1", &["seen"])]);

        assert!(pushes.is_none());
        assert_eq!(report.conflicts, 0);
        let converged = upserted(&writes, "1").expect("a converged placement");
        assert_eq!(converged.status, ReplicaStatus::Clean);
        assert!(converged.base.is_some(), "it becomes based on the remote");
    }

    #[test]
    fn an_unresolved_conflict_tracks_the_latest_remote_revision() {
        // A conflicted placement is left alone, but the observed remote
        // revision follows the remote so the consumer resolves against the
        // latest content.
        let mut placement = edited("1");
        placement.status = ReplicaStatus::Conflict;
        placement.conflict_revision = Some("r2".into());

        let mut sync = ReplicaSync::new("inbox", ReplicaSyncOptions::default());
        let (pushes, writes, report) = run(&mut sync, vec![placement], vec![remote_rev("1", "r3")]);

        assert!(pushes.is_none());
        assert_eq!(report.conflicts, 0, "no recount");
        let tracked = upserted(&writes, "1").expect("an updated placement");
        assert_eq!(tracked.status, ReplicaStatus::Conflict);
        assert_eq!(tracked.conflict_revision.as_deref(), Some("r3"));
        assert_eq!(
            tracked.object,
            Some(ReplicaHash::from("h2")),
            "the edit survives"
        );
    }

    #[test]
    fn a_content_conflict_still_pulls_the_remote_flag_change() {
        // Content diverged AND the remote changed flags in the same delta
        // row: the conflict mark must not eat the flag change, because a
        // delta lists it exactly once and skipping the flag merge would
        // diverge the replica for good.
        let mut sync = ReplicaSync::new("inbox", ReplicaSyncOptions::default());
        let mut item = remote_rev("1", "r2");
        item.flags = ReplicaFlags::from_iter(["seen"]);
        let (pushes, writes, report) = run(&mut sync, vec![edited("1")], vec![item]);

        assert!(pushes.is_none());
        assert_eq!(report.conflicts, 1);
        let conflicted = writes
            .iter()
            .rev()
            .find_map(|w| match w {
                ReplicaWriteOp::UpsertPlacement(p) if p.handle.as_str() == "1" => Some(p),
                _ => None,
            })
            .expect("a conflict write");
        assert_eq!(conflicted.status, ReplicaStatus::Conflict);
        assert_eq!(conflicted.conflict_revision.as_deref(), Some("r2"));
        assert!(conflicted.flags.contains("seen"), "the flag change lands");
        assert_eq!(
            conflicted.object,
            Some(ReplicaHash::from("h2")),
            "the edit survives"
        );
    }

    #[test]
    fn unlisted_conflict_keeps_its_observed_remote_revision() {
        // A conflicted placement unlisted by a delta is unchanged upstream
        // since the cursor: the synthesized remote state must carry the
        // observed conflict revision, not the stale base one, or the
        // conflict tracking would regress and the resolution would push
        // against the wrong precondition.
        let mut placement = edited("1");
        placement.status = ReplicaStatus::Conflict;
        placement.conflict_revision = Some("r2".into());

        let mut sync = ReplicaSync::new("inbox", ReplicaSyncOptions::default());
        let snapshot = delta(vec![], vec![]);
        let (pushes, writes, report) = run_snapshot(&mut sync, vec![placement], snapshot);

        assert!(pushes.is_none());
        assert_eq!(report.conflicts, 0, "no recount");
        assert!(
            upserted(&writes, "1").is_none(),
            "the conflict tracking must not regress to the base revision: {writes:?}",
        );
    }

    #[test]
    fn remote_content_change_beats_a_local_delete() {
        // The remote edited what was deleted locally: the update wins, so
        // no Remove is pushed and the placement is re-pulled fresh on the
        // new revision.
        let mut placement = edited("1");
        placement.status = ReplicaStatus::Tombstone;

        let mut sync = ReplicaSync::new("inbox", ReplicaSyncOptions::default());
        let (pushes, writes, report) = run(&mut sync, vec![placement], vec![remote_rev("1", "r2")]);

        assert!(pushes.is_none(), "the delete is not pushed");
        assert_eq!(report.pulled, 1);
        let resurrected = upserted(&writes, "1").expect("a resurrected placement");
        assert_eq!(resurrected.status, ReplicaStatus::Clean);
        let base = resurrected.base.as_ref().expect("a base");
        assert_eq!(base.revision.as_deref(), Some("r2"));
    }

    #[test]
    fn move_with_staged_edit_pushes_the_edit_before_the_move() {
        // A moved placement carrying a staged edit pushes the Update
        // first, so the relocated member holds the edited body; the
        // tombstone survives the rebase and the move derives next sync.
        let mut placement = edited("1");
        placement.status = ReplicaStatus::Tombstone;
        placement.origin = Some(ReplicaOrigin {
            collection: "archive".into(),
            handle: ReplicaHandle::from("1"),
        });

        let mut first = ReplicaSync::new("inbox", ReplicaSyncOptions::default());
        let _ = first.resume(None);
        let _ = first.resume(Some(ReplicaArg::Load(ReplicaLoaded {
            placements: vec![placement],
            checkpoint: None,
        })));
        let pushes = match first.resume(Some(ReplicaArg::Enumerate(full(vec![remote_rev(
            "1", "r1",
        )])))) {
            ReplicaCoroutineState::Yielded(ReplicaYield::WantsPush { changes, .. }) => changes,
            state => panic!("expected WantsPush, got {state:?}"),
        };
        match &pushes[0] {
            ReplicaChange::Update {
                object, if_match, ..
            } => {
                assert_eq!(object, &ReplicaHash::from("h2"), "the edit pushes first");
                assert_eq!(if_match.as_deref(), Some("r1"));
            }
            other => panic!("expected an Update push, got {other:?}"),
        }

        let results = vec![ReplicaPushResult {
            handle: ReplicaHandle::from("1"),
            outcome: ReplicaPushOutcome::Accepted,
            assigned: None,
            revision: Some("r2".into()),
        }];
        let writes = match first.resume(Some(ReplicaArg::Push(results))) {
            ReplicaCoroutineState::Yielded(ReplicaYield::WantsWrite(writes)) => writes,
            state => panic!("expected WantsWrite, got {state:?}"),
        };
        let rebased = upserted(&writes, "1").expect("a rebased placement");
        assert_eq!(
            rebased.status,
            ReplicaStatus::Tombstone,
            "the move stays pending"
        );
        let base = rebased.base.as_ref().expect("a base");
        assert_eq!(base.revision.as_deref(), Some("r2"));
        assert_eq!(base.object, Some(ReplicaHash::from("h2")));

        // second sync: the edit landed, so the move itself derives now,
        // gated on the pushed revision
        let mut second = ReplicaSync::new("inbox", ReplicaSyncOptions::default());
        let (pushes, _writes, _report) = run(
            &mut second,
            vec![rebased.clone()],
            vec![remote_rev("1", "r2")],
        );
        match &pushes.expect("a push")[0] {
            ReplicaChange::Remove {
                to: Some(to),
                if_match,
                ..
            } => {
                assert_eq!(to.as_str(), "archive");
                assert_eq!(if_match.as_deref(), Some("r2"));
            }
            other => panic!("expected a move Remove, got {other:?}"),
        }
    }

    #[test]
    fn remove_carries_the_base_revision_as_precondition() {
        // A pushed delete is gated on the last-synced revision, so a
        // remote edit racing the delete is rejected server-side.
        let mut placement = edited("1");
        placement.status = ReplicaStatus::Tombstone;

        let mut sync = ReplicaSync::new("inbox", ReplicaSyncOptions::default());
        let (pushes, _writes, _report) =
            run(&mut sync, vec![placement], vec![remote_rev("1", "r1")]);

        match &pushes.expect("a push")[0] {
            ReplicaChange::Remove { if_match, .. } => assert_eq!(if_match.as_deref(), Some("r1")),
            other => panic!("expected a Remove push, got {other:?}"),
        }
    }

    #[test]
    fn remote_delete_with_staged_edit_resurrects_as_create() {
        // The remote deleted what was edited locally: the edit wins over
        // the delete (the mirror of remote-update-beats-local-delete), so
        // the placement converts to a pending create that re-uploads the
        // edited body instead of being silently dropped.
        let mut sync = ReplicaSync::new("inbox", ReplicaSyncOptions::default());
        let (pushes, writes, _report) = run(&mut sync, vec![edited("1")], vec![]);

        match &pushes.expect("a push")[0] {
            ReplicaChange::Add { object, origin, .. } => {
                assert_eq!(object, &Some(ReplicaHash::from("h2")), "the edited body");
                assert!(origin.is_none(), "an append, not a copy");
            }
            other => panic!("expected an Add push, got {other:?}"),
        }
        let resurrected = upserted(&writes, "1").expect("a resurrected placement");
        assert_eq!(resurrected.status, ReplicaStatus::Created);
        assert!(resurrected.base.is_none());
        assert_eq!(resurrected.object, Some(ReplicaHash::from("h2")));
    }

    #[test]
    fn remote_delete_of_a_conflicted_placement_resurrects_the_edit() {
        // The remote deleted the item while a content conflict was
        // pending: the conflict is moot (the remote side is gone), and
        // the surviving local edit wins by resurrecting as a pending
        // create rather than being dropped with the placement.
        let mut placement = edited("1");
        placement.status = ReplicaStatus::Conflict;
        placement.conflict_revision = Some("r2".into());

        let mut sync = ReplicaSync::new("inbox", ReplicaSyncOptions::default());
        let (pushes, writes, _report) = run(&mut sync, vec![placement], vec![]);

        assert!(matches!(
            &pushes.expect("a push")[0],
            ReplicaChange::Add { origin: None, .. }
        ));
        let resurrected = upserted(&writes, "1").expect("a resurrected placement");
        assert_eq!(resurrected.status, ReplicaStatus::Created);
        assert_eq!(resurrected.conflict_revision, None, "the conflict is moot");
        assert_eq!(
            resurrected.object,
            Some(ReplicaHash::from("h2")),
            "the edit survives"
        );
    }

    #[test]
    fn read_only_remote_delete_with_staged_edit_keeps_the_edit() {
        // Same rescue on a read-only source: no push, but the placement
        // still converts to a pending create so the edit survives for a
        // later writable sync instead of being dropped.
        let opts = ReplicaSyncOptions {
            push: false,
            rights: ReplicaPushRights::all(),
            conflict: ReplicaConflictPolicy::Manual,
            full: false,
        };
        let mut sync = ReplicaSync::new("inbox", opts);
        let (pushes, writes, _report) = run(&mut sync, vec![edited("1")], vec![]);

        assert!(pushes.is_none());
        let resurrected = upserted(&writes, "1").expect("a resurrected placement");
        assert_eq!(resurrected.status, ReplicaStatus::Created);
        assert_eq!(resurrected.object, Some(ReplicaHash::from("h2")));
    }

    #[test]
    fn read_only_keeps_a_content_edit_dirty() {
        // A read-only source never pushes: the staged edit stays dirty for
        // a later writable sync.
        let mut sync = ReplicaSync::new(
            "inbox",
            ReplicaSyncOptions {
                push: false,
                rights: ReplicaPushRights::all(),
                conflict: ReplicaConflictPolicy::Manual,
                full: false,
            },
        );
        let (pushes, writes, _report) =
            run(&mut sync, vec![edited("1")], vec![remote_rev("1", "r1")]);

        assert!(pushes.is_none());
        assert!(
            upserted(&writes, "1").is_none(),
            "the placement is left as is"
        );
    }

    #[test]
    fn read_only_delete_applies_locally_without_push() {
        // A local delete on a read-only source can never propagate: the
        // replica mirrors the source, so the delete is applied locally
        // only and the next enumerate re-adds the member.
        let mut local = synced("1", &["seen"]);
        local.status = ReplicaStatus::Tombstone;

        let opts = ReplicaSyncOptions {
            push: false,
            rights: ReplicaPushRights::all(),
            conflict: ReplicaConflictPolicy::Manual,
            full: false,
        };
        let mut sync = ReplicaSync::new("inbox", opts);
        let (pushes, writes, report) = run(&mut sync, vec![local], vec![remote("1", &["seen"])]);

        assert!(pushes.is_none(), "read-only source must not push");
        assert_eq!(report.pushed, 0);
        assert!(
            writes.iter().any(
                |w| matches!(w, ReplicaWriteOp::DropPlacement { handle, .. } if handle.as_str() == "1")
            ),
            "the tombstone is applied locally: {writes:?}",
        );
    }

    #[test]
    fn unknown_vanished_handle_is_ignored() {
        // A delta may report a vanished handle the replica never knew
        // (removed before it was ever enumerated locally): nothing to
        // merge, nothing to write, no panic.
        let mut sync = ReplicaSync::new("inbox", ReplicaSyncOptions::default());
        let snapshot = delta(vec![], vec![ReplicaHandle::from("ghost")]);
        let (pushes, writes, report) = run_snapshot(&mut sync, vec![], snapshot);

        assert!(pushes.is_none());
        assert_eq!(report, ReplicaSyncReport::default());
        assert_eq!(writes.len(), 1, "only the checkpoint write");
        assert!(matches!(&writes[0], ReplicaWriteOp::SetCheckpoint { .. }));
    }

    #[test]
    fn noop_flag_edit_rebases_clean() {
        // A flag set put back to its base value is a no-op edit: nothing
        // to push, but the placement must come out clean rather than stay
        // dirty forever.
        let mut local = synced("1", &["seen"]);
        local.status = ReplicaStatus::Dirty;

        let mut sync = ReplicaSync::new("inbox", ReplicaSyncOptions::default());
        let (pushes, writes, report) = run(&mut sync, vec![local], vec![remote("1", &["seen"])]);

        assert!(pushes.is_none(), "nothing to push");
        assert_eq!(report, ReplicaSyncReport::default());
        let cleaned = upserted(&writes, "1").expect("a cleaning rebase");
        assert_eq!(cleaned.status, ReplicaStatus::Clean);
    }

    #[test]
    fn missing_arg_errors() {
        let mut sync = ReplicaSync::new("inbox", ReplicaSyncOptions::default());
        let _ = sync.resume(None);
        match sync.resume(None) {
            ReplicaCoroutineState::Complete(Err(ReplicaSyncError::MissingArg)) => {}
            state => panic!("expected MissingArg, got {state:?}"),
        }
    }

    #[test]
    fn unexpected_arg_errors() {
        let mut sync = ReplicaSync::new("inbox", ReplicaSyncOptions::default());
        let _ = sync.resume(None);
        match sync.resume(Some(ReplicaArg::Write)) {
            ReplicaCoroutineState::Complete(Err(ReplicaSyncError::UnexpectedArg)) => {}
            state => panic!("expected UnexpectedArg, got {state:?}"),
        }
    }

    /// A writable sync (`push = true`) with the given per-kind rights.
    fn with_rights(flags: bool, content: bool, add: bool, remove: bool) -> ReplicaSyncOptions {
        ReplicaSyncOptions {
            rights: ReplicaPushRights {
                flags,
                content,
                add,
                remove,
            },
            ..Default::default()
        }
    }

    #[test]
    fn forbidding_flags_keeps_dirty_without_push() {
        let mut local = synced("1", &[]);
        local.flags = ReplicaFlags::from_iter(["seen"]);
        local.status = ReplicaStatus::Dirty;

        let mut sync = ReplicaSync::new("inbox", with_rights(false, true, true, true));
        let (pushes, _writes, report) = run(&mut sync, vec![local], vec![remote("1", &[])]);

        assert!(pushes.is_none(), "a forbidden flag push must not fire");
        assert_eq!(report.pushed, 0);
    }

    #[test]
    fn forbidding_remove_keeps_tombstone_undropped() {
        let mut local = synced("1", &["seen"]);
        local.status = ReplicaStatus::Tombstone;

        let mut sync = ReplicaSync::new("inbox", with_rights(true, true, true, false));
        let (pushes, writes, report) = run(&mut sync, vec![local], vec![remote("1", &["seen"])]);

        assert!(pushes.is_none(), "a forbidden remove must not push");
        assert!(
            !writes.iter().any(|w| matches!(
                w,
                ReplicaWriteOp::DropPlacement { handle, .. } if handle.as_str() == "1"
            )),
            "the tombstone must be retained, not dropped: {writes:?}",
        );
        assert_eq!(report.pushed, 0);
    }

    #[test]
    fn flags_allowed_remove_forbidden_pushes_only_flags() {
        let mut dirty = synced("1", &[]);
        dirty.flags = ReplicaFlags::from_iter(["seen"]);
        dirty.status = ReplicaStatus::Dirty;
        let mut tomb = synced("2", &[]);
        tomb.status = ReplicaStatus::Tombstone;

        let mut sync = ReplicaSync::new("inbox", with_rights(true, true, true, false));
        let (pushes, _writes, _report) = run(
            &mut sync,
            vec![dirty, tomb],
            vec![remote("1", &[]), remote("2", &[])],
        );

        let pushes = pushes.expect("the permitted flag push still fires");
        assert!(
            pushes
                .iter()
                .all(|c| matches!(c, ReplicaChange::SetFlags { .. })),
            "only the flag change may be pushed, not the delete: {pushes:?}",
        );
    }

    #[test]
    fn forbidding_add_keeps_created_pending() {
        let mut sync = ReplicaSync::new("inbox", with_rights(true, true, false, true));
        let (pushes, _writes, report) = run(&mut sync, vec![created("tmp")], vec![]);

        assert!(pushes.is_none(), "a forbidden add must not push the create");
        assert_eq!(report.pushed, 0);
    }

    #[test]
    fn event_added_on_remote_add() {
        let mut sync = ReplicaSync::new("inbox", ReplicaSyncOptions::default());
        let (_p, _w, report) = run(&mut sync, vec![], vec![remote("1", &["seen"])]);
        assert_eq!(
            report.events,
            vec![ReplicaEvent::Added(ReplicaHandle::from("1"))]
        );
    }

    #[test]
    fn event_flags_changed_on_remote_flag_pull() {
        let local = synced("1", &[]);
        let mut sync = ReplicaSync::new("inbox", ReplicaSyncOptions::default());
        let (_p, _w, report) = run(&mut sync, vec![local], vec![remote("1", &["seen"])]);
        assert_eq!(
            report.events,
            vec![ReplicaEvent::FlagsChanged(ReplicaHandle::from("1"))]
        );
    }

    #[test]
    fn event_vanished_on_delta_vanish() {
        let local = synced("1", &["seen"]);
        let mut sync = ReplicaSync::new("inbox", ReplicaSyncOptions::default());
        let snapshot = delta(vec![], vec![ReplicaHandle::from("1")]);
        let (_p, _w, report) = run_snapshot(&mut sync, vec![local], snapshot);
        assert_eq!(
            report.events,
            vec![ReplicaEvent::Vanished(ReplicaHandle::from("1"))]
        );
    }

    #[test]
    fn event_flags_changed_on_accepted_push() {
        let mut local = synced("1", &[]);
        local.flags = ReplicaFlags::from_iter(["seen"]);
        local.status = ReplicaStatus::Dirty;
        let mut sync = ReplicaSync::new("inbox", ReplicaSyncOptions::default());
        let (_p, _w, report) = run(&mut sync, vec![local], vec![remote("1", &[])]);
        assert_eq!(
            report.events,
            vec![ReplicaEvent::FlagsChanged(ReplicaHandle::from("1"))]
        );
        assert_eq!(report.pushed, 1);
    }

    #[test]
    fn event_created_on_accepted_create() {
        let mut sync = ReplicaSync::new("inbox", ReplicaSyncOptions::default());
        let results = vec![ReplicaPushResult {
            handle: ReplicaHandle::from("tmp"),
            outcome: ReplicaPushOutcome::Accepted,
            assigned: Some(ReplicaHandle::from("99")),
            revision: None,
        }];
        let (_w, report) = drive_push(&mut sync, vec![created("tmp")], vec![], results);
        // the create is reported under the server-assigned handle
        assert_eq!(
            report.events,
            vec![ReplicaEvent::Created(ReplicaHandle::from("99"))]
        );
    }

    // headless conflict policy

    /// Sync options with a conflict policy, everything else default.
    fn with_conflict(policy: ReplicaConflictPolicy) -> ReplicaSyncOptions {
        ReplicaSyncOptions {
            conflict: policy,
            ..Default::default()
        }
    }

    #[test]
    fn prefer_remote_drops_the_local_edit() {
        let mut sync =
            ReplicaSync::new("inbox", with_conflict(ReplicaConflictPolicy::PreferRemote));
        let (pushes, writes, report) =
            run(&mut sync, vec![edited("1")], vec![remote_rev("1", "r2")]);

        assert!(pushes.is_none(), "prefer-remote pulls, never pushes");
        assert_eq!(report.conflicts, 0);
        assert_eq!(report.refreshed, 1, "the remote content is pulled");
        let pulled = upserted(&writes, "1").expect("a pulled placement");
        assert_eq!(pulled.object, None, "the local edit is dropped");
        assert_eq!(pulled.level, ReplicaLevel::Probed);
    }

    #[test]
    fn prefer_local_overwrites_the_remote() {
        let mut sync = ReplicaSync::new("inbox", with_conflict(ReplicaConflictPolicy::PreferLocal));
        let (pushes, _writes, report) =
            run(&mut sync, vec![edited("1")], vec![remote_rev("1", "r2")]);

        let pushes = pushes.expect("prefer-local pushes the edit");
        match &pushes[0] {
            ReplicaChange::Update {
                object, if_match, ..
            } => {
                assert_eq!(object, &ReplicaHash::from("h2"));
                assert_eq!(
                    if_match.as_deref(),
                    Some("r2"),
                    "overwrites the current remote revision, not the stale base",
                );
            }
            other => panic!("expected an Update push, got {other:?}"),
        }
        assert_eq!(report.conflicts, 0);
    }

    #[test]
    fn prefer_local_falls_back_to_conflict_when_it_cannot_push() {
        let opts = ReplicaSyncOptions {
            conflict: ReplicaConflictPolicy::PreferLocal,
            rights: ReplicaPushRights {
                content: false,
                ..ReplicaPushRights::all()
            },
            ..Default::default()
        };
        let mut sync = ReplicaSync::new("inbox", opts);
        let (pushes, _writes, report) =
            run(&mut sync, vec![edited("1")], vec![remote_rev("1", "r2")]);

        assert!(pushes.is_none());
        assert_eq!(report.conflicts, 1, "no push right, so it stays a conflict");
    }

    #[test]
    fn keep_both_pulls_the_remote_and_stages_the_local_body() {
        let mut sync = ReplicaSync::new("inbox", with_conflict(ReplicaConflictPolicy::KeepBoth));
        let (pushes, writes, report) =
            run(&mut sync, vec![edited("1")], vec![remote_rev("1", "r2")]);

        assert!(
            pushes.is_none(),
            "the duplicate is staged, pushed next sync"
        );
        assert_eq!(report.conflicts, 0);
        assert_eq!(
            report.refreshed, 1,
            "the remote is pulled into the placement"
        );
        let dup = writes
            .iter()
            .find_map(|w| match w {
                ReplicaWriteOp::UpsertPlacement(p) if p.status == ReplicaStatus::Created => Some(p),
                _ => None,
            })
            .expect("a keep-both duplicate");
        assert_eq!(
            dup.object,
            Some(ReplicaHash::from("h2")),
            "the duplicate carries the local body",
        );
        assert!(dup.handle.as_str().contains("keepboth"));
    }
}
