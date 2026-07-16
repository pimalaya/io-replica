//! I/O-free coroutine to reconcile a collection with its remote.
//!
//! The load-bearing verb. It loads local state, enumerates the remote
//! delta, then runs a three-way merge of Local, OfflineBase and OfflineRemote per
//! placement: local-won changes are pushed, remote-won changes are
//! pulled. OfflineFlags merge element-wise and never conflict (each flag is
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

use core::{fmt, mem};

use alloc::{collections::BTreeMap, collections::BTreeSet, string::String, vec::Vec};

use log::{debug, trace};
use thiserror::Error;

use crate::{
    change::{OfflineChange, OfflineWriteOp},
    collection::{OfflineCheckpoint, OfflineCollectionId},
    coroutine::*,
    placement::{
        OfflineBase, OfflineFlags, OfflineHandle, OfflineLevel, OfflinePlacement, OfflineStatus,
    },
    remote::{OfflinePushOutcome, OfflineRemoteItem, OfflineRemoteSnapshot},
};

/// Tuning for one sync run: the push direction and the enumerate depth.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct OfflineSyncOptions {
    /// When false the source is treated read-only: local flag and content
    /// changes are kept dirty and never pushed (permission gating). Local
    /// deletes instead apply locally only, deliberately: they can never
    /// propagate, so the replica mirrors the source and the next
    /// enumerate re-adds the member.
    pub push: bool,
    /// When true the checkpoint is ignored and the whole remote is
    /// enumerated, so the merge reconciles the complete spine: it re-adds
    /// any locally-missing message and drops any local phantom. The
    /// recovery path for a replica that drifted out of sync.
    pub full: bool,
}

impl Default for OfflineSyncOptions {
    fn default() -> Self {
        Self {
            push: true,
            full: false,
        }
    }
}

/// What a sync did.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct OfflineSyncReport {
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
}

/// Failure causes during a SYNC flow.
#[derive(Clone, Debug, Error)]
pub enum OfflineSyncError {
    /// The driver fed back an arg that does not match the pending yield.
    #[error("Offline SYNC failed: unexpected coroutine arg")]
    UnexpectedArg,
    /// The driver resumed without the arg the pending yield required.
    #[error("Offline SYNC failed: missing coroutine arg")]
    MissingArg,
}

/// I/O-free SYNC coroutine.
pub struct OfflineSync {
    collection: OfflineCollectionId,
    opts: OfflineSyncOptions,
    local: BTreeMap<OfflineHandle, OfflinePlacement>,
    checkpoint: Option<OfflineCheckpoint>,
    writes: Vec<OfflineWriteOp>,
    /// Flag pushes awaiting their outcome: the local placement to rebase
    /// once the remote confirms, keyed by handle. A rejected push leaves
    /// the placement untouched (dirty) so the next sync retries it.
    pending_flag_pushes: BTreeMap<OfflineHandle, OfflinePlacement>,
    /// Content pushes awaiting their outcome: the local placement whose
    /// base adopts the pushed body and reported revision once the remote
    /// confirms. Kept apart from flag pushes so an accepted flag push on
    /// a conflicted placement (whose body also differs from its base) is
    /// never misread as a resolved content push.
    pending_content_pushes: BTreeMap<OfflineHandle, OfflinePlacement>,
    /// Tombstone deletes awaiting their outcome: the placement is dropped
    /// only once the remote confirms the delete. A rejected push keeps the
    /// tombstone so the next sync retries, rather than dropping a message
    /// the server still has (a permanent desync under incremental sync).
    pending_drops: BTreeSet<OfflineHandle>,
    /// Pending creates awaiting their outcome, keyed by provisional handle:
    /// the staged placement to rekey to the server-assigned handle once the
    /// add is accepted. A rejected push keeps the placeholder for a retry.
    pending_creates: BTreeMap<OfflineHandle, OfflinePlacement>,
    report: OfflineSyncReport,
    state: State,
}

impl OfflineSync {
    /// Creates a coroutine that reconciles `collection`.
    pub fn new(collection: impl Into<OfflineCollectionId>, opts: OfflineSyncOptions) -> Self {
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
            report: OfflineSyncReport::default(),
            state: State::Start,
        }
    }

    /// Runs the three-way merge, filling `self.writes` and `self.report`
    /// and returning the pushes to send.
    fn reconcile(&mut self, snapshot: OfflineRemoteSnapshot) -> Vec<OfflineChange> {
        let OfflineRemoteSnapshot {
            items,
            vanished,
            complete,
            checkpoint,
        } = snapshot;

        let remote: BTreeMap<OfflineHandle, OfflineRemoteItem> =
            items.into_iter().map(|i| (i.handle.clone(), i)).collect();
        let vanished: BTreeSet<OfflineHandle> = vanished.into_iter().collect();

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

        self.writes.push(OfflineWriteOp::SetCheckpoint {
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
        remote: &BTreeMap<OfflineHandle, OfflineRemoteItem>,
    ) -> Vec<(OfflineHandle, Option<OfflineRemoteItem>)> {
        let handles: BTreeSet<OfflineHandle> =
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
        remote: &BTreeMap<OfflineHandle, OfflineRemoteItem>,
        vanished: &BTreeSet<OfflineHandle>,
    ) -> Vec<(OfflineHandle, Option<OfflineRemoteItem>)> {
        let dirty = self
            .local
            .iter()
            .filter(|(_, p)| p.status != OfflineStatus::Clean)
            .map(|(handle, _)| handle.clone());

        let handles: BTreeSet<OfflineHandle> = remote
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
                        Some(OfflineRemoteItem {
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
        handle: &OfflineHandle,
        remote_item: Option<OfflineRemoteItem>,
    ) -> Option<OfflineChange> {
        let local = self.local.get(handle).cloned();

        let based = local.as_ref().map(|p| p.base.is_some()).unwrap_or(false);

        let local_tombstone = local
            .as_ref()
            .map(|p| p.status == OfflineStatus::Tombstone)
            .unwrap_or(false);
        let local_present = local.is_some() && !local_tombstone;
        let remote_present = remote_item.is_some();

        match (local_present, based, remote_present) {
            // local delete or move: both knew it, we removed it here
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
                    return None;
                }

                if !self.opts.push {
                    // Read-only source: apply the delete locally only.
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
                    self.pending_content_pushes
                        .insert(handle.clone(), local.clone());
                    return Some(OfflineChange::Update {
                        handle: handle.clone(),
                        object: local.object.clone().expect("a staged edited body"),
                        if_match: base_revision,
                    });
                }

                // Hold the drop until the remote confirms it (in PendingPush);
                // a rejected push keeps the tombstone for the next retry. A
                // move carries its destination in `origin`; a plain delete has
                // none, and the consumer routes it to trash.
                self.pending_drops.insert(handle.clone());
                let to = local.origin.as_ref().map(|o| o.collection.clone());
                Some(OfflineChange::Remove {
                    handle: handle.clone(),
                    to,
                    if_match: base_revision,
                })
            }
            // local delete of something already gone remote
            (false, _, false) if local_tombstone => {
                self.drop(handle);
                None
            }
            // remote delete: we had it in sync, it vanished upstream
            (true, true, false) => {
                let local = local.as_ref().expect("local present");

                // NOTE: the edit-beats-delete rule, push direction: a
                // staged local edit survives the remote delete as a
                // pending create that re-uploads the edited body, rather
                // than being silently dropped with the placement. A
                // conflicted placement holds such an edit too, and the
                // remote deleting its side makes the conflict moot: the
                // surviving local edit wins by default.
                let edited = matches!(local.status, OfflineStatus::Dirty | OfflineStatus::Conflict)
                    && local.object.is_some()
                    && local
                        .base
                        .as_ref()
                        .is_some_and(|b| local.object != b.object);
                if edited {
                    let mut resurrected = local.clone();
                    resurrected.status = OfflineStatus::Created;
                    resurrected.conflict_revision = None;
                    resurrected.base = None;
                    resurrected.origin = None;
                    self.writes
                        .push(OfflineWriteOp::UpsertPlacement(resurrected.clone()));

                    if !self.opts.push {
                        return None;
                    }
                    self.pending_creates.insert(handle.clone(), resurrected);
                    return Some(OfflineChange::Add {
                        handle: handle.clone(),
                        link_id: local.link_id.clone(),
                        flags: local.flags.clone(),
                        origin: None,
                        object: local.object.clone(),
                    });
                }

                self.drop(handle);
                self.report.pulled += 1;
                None
            }
            // remote add: brand new upstream
            (false, false, true) => {
                self.pull_add(&remote_item.expect("remote present"));
                self.report.pulled += 1;
                None
            }
            // present on both: reconcile content, then flags
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
            // local create (no base, not upstream): a pending copy/move or
            // append. Stage the add; the rekey to the server-assigned handle
            // is held until the push reports it (in PendingPush). A plain
            // base-less placement that is not Created is left untouched.
            (true, false, false) => {
                let local = local.as_ref().expect("local present");
                if self.opts.push && local.status == OfflineStatus::Created {
                    self.pending_creates.insert(handle.clone(), local.clone());
                    return Some(OfflineChange::Add {
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
        local: &OfflinePlacement,
        item: &OfflineRemoteItem,
    ) -> ContentOutcome {
        let Some(base) = &local.base else {
            // A base-less placement that carries a body the remote also
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
                conflicted.status = OfflineStatus::Conflict;
                conflicted.conflict_revision = item.revision.clone();
                self.writes
                    .push(OfflineWriteOp::UpsertPlacement(conflicted.clone()));
                self.report.conflicts += 1;
                return ContentOutcome::Rewritten(conflicted);
            }
            return ContentOutcome::Untouched;
        };

        if local.status == OfflineStatus::Conflict {
            // NOTE: an unresolved conflict is left alone (the local edit
            // must survive), but the observed remote revision is kept
            // current so the consumer resolves against the latest remote.
            if item.revision.is_some() && item.revision != local.conflict_revision {
                let mut updated = local.clone();
                updated.conflict_revision = item.revision.clone();
                self.writes
                    .push(OfflineWriteOp::UpsertPlacement(updated.clone()));
                return ContentOutcome::Rewritten(updated);
            }
            return ContentOutcome::Untouched;
        }

        let local_changed = local.status == OfflineStatus::Dirty
            && local.object.is_some()
            && local.object != base.object;
        let remote_changed = item.revision.is_some() && item.revision != base.revision;

        match (local_changed, remote_changed) {
            (false, false) => ContentOutcome::Untouched,
            (false, true) => ContentOutcome::Rewritten(self.pull_content(local, item)),
            (true, false) => {
                if !self.opts.push {
                    // NOTE: read-only source, keep dirty and do not push
                    return ContentOutcome::Untouched;
                }
                self.pending_content_pushes
                    .insert(local.handle.clone(), local.clone());
                ContentOutcome::Push(OfflineChange::Update {
                    handle: local.handle.clone(),
                    object: local.object.clone().expect("a staged edited body"),
                    if_match: base.revision.clone(),
                })
            }
            (true, true) => {
                let mut conflicted = local.clone();
                conflicted.status = OfflineStatus::Conflict;
                conflicted.conflict_revision = item.revision.clone();
                self.writes
                    .push(OfflineWriteOp::UpsertPlacement(conflicted.clone()));
                self.report.conflicts += 1;
                ContentOutcome::Rewritten(conflicted)
            }
        }
    }

    /// Reconciles the flag sets of a placement present on both sides,
    /// returning a push when the local side won any flag.
    ///
    /// OfflineFlags merge element-wise ([`OfflineFlags::merge`]) and never conflict:
    /// divergent sets fold into one merged set that both sides converge
    /// on, pulling the remote-won flags and pushing the local-won ones.
    fn reconcile_flags(
        &mut self,
        local: &OfflinePlacement,
        remote: &OfflineRemoteItem,
    ) -> Option<OfflineChange> {
        let base_flags = local.base.as_ref().map(|b| b.flags.clone());

        let Some(base_flags) = base_flags else {
            // NOTE: never based but present on both: converge on remote
            self.pull_flags(local, &remote.flags);
            self.report.pulled += 1;
            return None;
        };

        let merged = OfflineFlags::merge(&base_flags, &local.flags, &remote.flags);
        let pull = merged != local.flags;
        let push = merged != remote.flags;

        match (pull, push) {
            // in sync, or both sides moved to the same flags: no push,
            // rebase when the shared move left the base behind, and clean
            // a dirty placement whose edit turned out to be a no-op (a
            // flag set put back to its base value would otherwise stay
            // dirty forever). A staged content edit is not a no-op: it
            // stays dirty (the read-only path lands here with one).
            (false, false) => {
                let content_edit = local.object.is_some()
                    && local
                        .base
                        .as_ref()
                        .is_some_and(|b| b.object != local.object);
                if local.flags != base_flags
                    || (local.status == OfflineStatus::Dirty && !content_edit)
                {
                    self.rebase(local, &merged);
                }
                None
            }
            // the remote won every differing flag: pull the merged set
            (true, false) => {
                self.pull_flags(local, &merged);
                self.report.pulled += 1;
                None
            }
            // the local side won at least one flag: push the merged set.
            // When the remote won some flags too, they are folded in
            // locally right away, dirty on the old base, so a rejected or
            // read-only push re-derives the same merge next sync.
            (pull, true) => {
                let mut updated = local.clone();
                updated.flags = merged.clone();
                if pull {
                    self.writes
                        .push(OfflineWriteOp::UpsertPlacement(updated.clone()));
                    self.report.pulled += 1;
                }

                if !self.opts.push {
                    // NOTE: read-only source, keep dirty and do not push
                    return None;
                }
                // Hold the rebase until the push is confirmed: a rejected
                // push must leave the placement dirty so the next sync
                // retries it, not rebase it onto a state the remote never
                // took (which QRESYNC would then never revisit).
                self.pending_flag_pushes
                    .insert(local.handle.clone(), updated);
                Some(OfflineChange::SetFlags {
                    handle: local.handle.clone(),
                    flags: merged,
                })
            }
        }
    }

    fn drop(&mut self, handle: &OfflineHandle) {
        self.writes.push(OfflineWriteOp::DropPlacement {
            collection: self.collection.clone(),
            handle: handle.clone(),
        });
    }

    fn pull_add(&mut self, item: &OfflineRemoteItem) {
        let placement = OfflinePlacement {
            collection: self.collection.clone(),
            handle: item.handle.clone(),
            link_id: None,
            object: None,
            level: OfflineLevel::Probed,
            meta: None,
            flags: item.flags.clone(),
            status: OfflineStatus::Clean,
            conflict_revision: None,
            base: Some(OfflineBase {
                flags: item.flags.clone(),
                revision: item.revision.clone(),
                object: None,
            }),
            origin: None,
        };
        self.writes.push(OfflineWriteOp::UpsertPlacement(placement));
    }

    /// Pulls a remote content change: the stale local body is dropped and
    /// the base adopts the new revision. The level falls back to probed
    /// (the cached summary describes the old revision; it is kept as a
    /// display fallback until a meta upgrade refetches it). OfflineFlags and
    /// status are left for the flag reconciliation to settle.
    fn pull_content(
        &mut self,
        local: &OfflinePlacement,
        item: &OfflineRemoteItem,
    ) -> OfflinePlacement {
        let mut updated = local.clone();
        updated.object = None;
        updated.level = OfflineLevel::Probed;
        if let Some(base) = &mut updated.base {
            base.revision = item.revision.clone();
            base.object = None;
        }

        self.writes
            .push(OfflineWriteOp::UpsertPlacement(updated.clone()));
        self.report.refreshed += 1;
        updated
    }

    /// Adopts `flags` as both the current and the base flag set. An
    /// unresolved content conflict keeps its status (only the flag axis
    /// converged), everything else lands clean.
    fn pull_flags(&mut self, local: &OfflinePlacement, flags: &OfflineFlags) {
        let mut updated = local.clone();
        updated.flags = flags.clone();
        if updated.status != OfflineStatus::Conflict {
            updated.status = OfflineStatus::Clean;
        }
        updated.base = Some(OfflineBase {
            flags: flags.clone(),
            revision: local.base.as_ref().and_then(|b| b.revision.clone()),
            object: local.base.as_ref().and_then(|b| b.object.clone()),
        });
        self.writes.push(OfflineWriteOp::UpsertPlacement(updated));
    }

    /// Rebases the flag axis onto `flags`, keeping the current flag set.
    /// An unresolved content conflict keeps its status (its resolution
    /// belongs to an edit), everything else lands clean.
    fn rebase(&mut self, local: &OfflinePlacement, flags: &OfflineFlags) {
        let mut updated = local.clone();
        if updated.status != OfflineStatus::Conflict {
            updated.status = OfflineStatus::Clean;
        }
        updated.base = Some(OfflineBase {
            flags: flags.clone(),
            revision: local.base.as_ref().and_then(|b| b.revision.clone()),
            object: local.base.as_ref().and_then(|b| b.object.clone()),
        });
        self.writes.push(OfflineWriteOp::UpsertPlacement(updated));
    }

    /// Rebases an accepted content push: the base pins the pushed body and
    /// the revision the remote reported. The base flags are left as they
    /// were, so a flag edit that rode along with the content edit stays
    /// derivable and pushes on the next sync.
    fn rebase_content(&mut self, local: &OfflinePlacement, revision: Option<String>) {
        let base_flags = local
            .base
            .as_ref()
            .map(|b| b.flags.clone())
            .unwrap_or_default();

        let mut updated = local.clone();
        // NOTE: a tombstone stays a tombstone: its edit pushed ahead of
        // the pending move, which derives on the next sync.
        updated.status = if local.status == OfflineStatus::Tombstone {
            OfflineStatus::Tombstone
        } else if local.flags == base_flags {
            OfflineStatus::Clean
        } else {
            OfflineStatus::Dirty
        };
        updated.base = Some(OfflineBase {
            flags: base_flags,
            revision,
            object: local.object.clone(),
        });
        self.writes.push(OfflineWriteOp::UpsertPlacement(updated));
    }

    /// Rekeys an accepted create: drops the provisional placeholder and
    /// upserts the same placement under the server-assigned `handle`, clean
    /// and based, so the next enumerate reconciles it as already in sync.
    /// The base records the revision the push reported and pins the pushed
    /// body: the remote now holds exactly that content.
    fn rekey_create(
        &mut self,
        placeholder: OfflinePlacement,
        handle: OfflineHandle,
        revision: Option<String>,
    ) {
        self.drop(&placeholder.handle);
        let mut placed = placeholder;
        placed.handle = handle;
        placed.status = OfflineStatus::Clean;
        placed.origin = None;
        placed.base = Some(OfflineBase {
            flags: placed.flags.clone(),
            revision,
            object: placed.object.clone(),
        });
        self.writes.push(OfflineWriteOp::UpsertPlacement(placed));
    }
}

impl OfflineCoroutine for OfflineSync {
    type Yield = OfflineYield;
    type Return = Result<OfflineSyncReport, OfflineSyncError>;

    fn resume(
        &mut self,
        arg: Option<OfflineArg>,
    ) -> OfflineCoroutineState<Self::Yield, Self::Return> {
        trace!("sync: {}", self.state);

        match (&self.state, arg) {
            (State::Start, None) => {
                debug!("load local state from storage");
                self.state = State::PendingLoad;
                OfflineCoroutineState::Yielded(OfflineYield::WantsLoad(self.collection.clone()))
            }

            (State::PendingLoad, Some(OfflineArg::Load(loaded))) => {
                self.local = loaded
                    .placements
                    .into_iter()
                    .map(|p| (p.handle.clone(), p))
                    .collect();
                // A full sync ignores the checkpoint, so the enumerate yields
                // the whole remote and the merge reconciles the entire spine.
                self.checkpoint = if self.opts.full {
                    None
                } else {
                    loaded.checkpoint
                };

                debug!("enumerate remote from checkpoint");
                trace!("loaded {} local items", self.local.len());
                self.state = State::PendingEnumerate;
                OfflineCoroutineState::Yielded(OfflineYield::WantsEnumerate {
                    collection: self.collection.clone(),
                    cursor: self.checkpoint.clone(),
                })
            }

            (State::PendingEnumerate, Some(OfflineArg::Enumerate(snapshot))) => {
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
                    return OfflineCoroutineState::Yielded(OfflineYield::WantsWrite(writes));
                }

                debug!("push {} local changes to remote", pushes.len());
                trace!("changes: {pushes:?}");
                self.state = State::PendingPush;
                OfflineCoroutineState::Yielded(OfflineYield::WantsPush {
                    collection: self.collection.clone(),
                    changes: pushes,
                })
            }

            (State::PendingPush, Some(OfflineArg::Push(results))) => {
                for result in &results {
                    match result.outcome {
                        // The remote took the change: rebase a flag or
                        // content push clean, drop a confirmed delete, or
                        // rekey a confirmed create to its server-assigned
                        // handle, so the local state matches what the
                        // remote now holds.
                        OfflinePushOutcome::Accepted => {
                            self.report.pushed += 1;
                            if let Some(placement) = self.pending_flag_pushes.remove(&result.handle)
                            {
                                let flags = placement.flags.clone();
                                self.rebase(&placement, &flags);
                            }
                            if let Some(placement) =
                                self.pending_content_pushes.remove(&result.handle)
                            {
                                self.rebase_content(&placement, result.revision.clone());
                            }
                            if self.pending_drops.remove(&result.handle) {
                                self.drop(&result.handle);
                            }
                            if let Some(placeholder) = self.pending_creates.remove(&result.handle) {
                                match result.assigned.clone() {
                                    // The remote returned the new handle: rekey.
                                    Some(assigned) => self.rekey_create(
                                        placeholder,
                                        assigned,
                                        result.revision.clone(),
                                    ),
                                    // No assigned handle (no UIDPLUS): the copy
                                    // landed, so drop the placeholder; the next
                                    // enumerate re-adds it by its real handle and
                                    // links the body by link id.
                                    None => self.drop(&placeholder.handle),
                                }
                            }
                        }
                        // The remote refused it: leave the dirty placement,
                        // tombstone or placeholder untouched so the next sync
                        // retries the push.
                        OfflinePushOutcome::Rejected => self.report.rejected += 1,
                    }
                }
                // Any handle the push never reported on stays pending too.
                self.pending_flag_pushes.clear();
                self.pending_content_pushes.clear();
                self.pending_drops.clear();
                self.pending_creates.clear();

                debug!("pushed, write {} storage ops", self.writes.len());
                trace!("push results: {results:?}");
                self.state = State::PendingWrite;
                let writes = mem::take(&mut self.writes);
                OfflineCoroutineState::Yielded(OfflineYield::WantsWrite(writes))
            }

            (State::PendingWrite, Some(OfflineArg::Write)) => {
                debug!(
                    "sync done: {} pulled, {} pushed, {} refreshed, {} conflicts, {} rejected",
                    self.report.pulled,
                    self.report.pushed,
                    self.report.refreshed,
                    self.report.conflicts,
                    self.report.rejected,
                );
                OfflineCoroutineState::Complete(Ok(self.report))
            }

            (_, Some(_)) => OfflineCoroutineState::Complete(Err(OfflineSyncError::UnexpectedArg)),
            (_, None) => OfflineCoroutineState::Complete(Err(OfflineSyncError::MissingArg)),
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
    Rewritten(OfflinePlacement),
    /// The local content won: the change to push.
    Push(OfflineChange),
}

enum State {
    Start,
    PendingLoad,
    PendingEnumerate,
    PendingPush,
    PendingWrite,
}

impl fmt::Display for State {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Start => f.write_str("start"),
            Self::PendingLoad => f.write_str("pending load"),
            Self::PendingEnumerate => f.write_str("pending enumerate"),
            Self::PendingPush => f.write_str("pending push"),
            Self::PendingWrite => f.write_str("pending write"),
        }
    }
}

#[cfg(test)]
mod tests {
    use alloc::{vec, vec::Vec};

    use crate::{
        object::OfflineHash,
        placement::{OfflineLinkId, OfflineOrigin},
        remote::{OfflinePushOutcome, OfflinePushResult, OfflineRemoteSnapshot},
        storage::OfflineLoaded,
        sync::*,
    };

    /// A pending create staged in "inbox", its body sourced from "sent".
    fn created(handle: &str) -> OfflinePlacement {
        OfflinePlacement {
            collection: "inbox".into(),
            handle: OfflineHandle::from(handle),
            link_id: None,
            object: None,
            level: OfflineLevel::Probed,
            meta: None,
            flags: OfflineFlags::default(),
            status: OfflineStatus::Created,
            conflict_revision: None,
            base: None,
            origin: Some(OfflineOrigin {
                collection: "sent".into(),
                handle: OfflineHandle::from("9"),
            }),
        }
    }

    fn synced(handle: &str, flags: &[&str]) -> OfflinePlacement {
        OfflinePlacement {
            collection: "inbox".into(),
            handle: OfflineHandle::from(handle),
            link_id: Some(OfflineLinkId::from(handle)),
            object: None,
            level: OfflineLevel::Probed,
            meta: None,
            flags: OfflineFlags::from_iter(flags.iter().copied()),
            status: OfflineStatus::Clean,
            conflict_revision: None,
            base: Some(OfflineBase {
                flags: OfflineFlags::from_iter(flags.iter().copied()),
                revision: None,
                object: None,
            }),
            origin: None,
        }
    }

    fn remote(handle: &str, flags: &[&str]) -> OfflineRemoteItem {
        OfflineRemoteItem {
            handle: OfflineHandle::from(handle),
            flags: OfflineFlags::from_iter(flags.iter().copied()),
            revision: None,
        }
    }

    fn full(items: Vec<OfflineRemoteItem>) -> OfflineRemoteSnapshot {
        OfflineRemoteSnapshot {
            items,
            vanished: Vec::new(),
            complete: true,
            checkpoint: OfflineCheckpoint(b"c1".to_vec()),
        }
    }

    fn delta(items: Vec<OfflineRemoteItem>, vanished: Vec<OfflineHandle>) -> OfflineRemoteSnapshot {
        OfflineRemoteSnapshot {
            items,
            vanished,
            complete: false,
            checkpoint: OfflineCheckpoint(b"c1".to_vec()),
        }
    }

    fn run(
        sync: &mut OfflineSync,
        local: Vec<OfflinePlacement>,
        items: Vec<OfflineRemoteItem>,
    ) -> (
        Option<Vec<OfflineChange>>,
        Vec<OfflineWriteOp>,
        OfflineSyncReport,
    ) {
        run_snapshot(sync, local, full(items))
    }

    fn run_snapshot(
        sync: &mut OfflineSync,
        local: Vec<OfflinePlacement>,
        snapshot: OfflineRemoteSnapshot,
    ) -> (
        Option<Vec<OfflineChange>>,
        Vec<OfflineWriteOp>,
        OfflineSyncReport,
    ) {
        crate::testlog::init();
        let _ = sync.resume(None);
        let _ = sync.resume(Some(OfflineArg::Load(OfflineLoaded {
            placements: local,
            checkpoint: None,
        })));

        let mut pushes = None;
        let writes = match sync.resume(Some(OfflineArg::Enumerate(snapshot))) {
            OfflineCoroutineState::Yielded(OfflineYield::WantsPush { changes, .. }) => {
                // the fake remote accepts everything, assigning nothing
                let results = changes
                    .iter()
                    .map(|change| {
                        let handle = match change {
                            OfflineChange::Add { handle, .. } => handle,
                            OfflineChange::Remove { handle, .. } => handle,
                            OfflineChange::SetFlags { handle, .. } => handle,
                            OfflineChange::Update { handle, .. } => handle,
                        };
                        OfflinePushResult {
                            handle: handle.clone(),
                            outcome: OfflinePushOutcome::Accepted,
                            assigned: None,
                            revision: None,
                        }
                    })
                    .collect();
                pushes = Some(changes);
                match sync.resume(Some(OfflineArg::Push(results))) {
                    OfflineCoroutineState::Yielded(OfflineYield::WantsWrite(w)) => w,
                    state => panic!("expected WantsWrite, got {state:?}"),
                }
            }
            OfflineCoroutineState::Yielded(OfflineYield::WantsWrite(w)) => w,
            state => panic!("expected push or write, got {state:?}"),
        };

        let report = match sync.resume(Some(OfflineArg::Write)) {
            OfflineCoroutineState::Complete(Ok(report)) => report,
            state => panic!("expected Complete(Ok), got {state:?}"),
        };

        (pushes, writes, report)
    }

    #[test]
    fn remote_add_pulls_probed() {
        crate::testlog::init();
        let mut sync = OfflineSync::new("inbox", OfflineSyncOptions::default());
        let (pushes, writes, report) = run(&mut sync, vec![], vec![remote("1", &["seen"])]);

        assert!(pushes.is_none());
        assert_eq!(report.pulled, 1);
        let OfflineWriteOp::UpsertPlacement(p) = &writes[0] else {
            panic!("expected UpsertPlacement, got {:?}", writes[0]);
        };
        assert_eq!(p.level, OfflineLevel::Probed);
        assert!(p.flags.contains("seen"));
    }

    #[test]
    fn local_flag_change_pushes() {
        let mut local = synced("1", &[]);
        local.flags = OfflineFlags::from_iter(["seen"]);
        local.status = OfflineStatus::Dirty;

        let mut sync = OfflineSync::new("inbox", OfflineSyncOptions::default());
        let (pushes, _writes, report) = run(&mut sync, vec![local], vec![remote("1", &[])]);

        let pushes = pushes.expect("a push");
        assert!(matches!(pushes[0], OfflineChange::SetFlags { .. }));
        assert_eq!(report.pushed, 1);
    }

    /// Drives a sync through its push, feeding back the given push results,
    /// and returns the storage writes the engine then stages and the report.
    fn drive_push(
        sync: &mut OfflineSync,
        local: Vec<OfflinePlacement>,
        items: Vec<OfflineRemoteItem>,
        results: Vec<OfflinePushResult>,
    ) -> (Vec<OfflineWriteOp>, OfflineSyncReport) {
        crate::testlog::init();
        let _ = sync.resume(None);
        let _ = sync.resume(Some(OfflineArg::Load(OfflineLoaded {
            placements: local,
            checkpoint: None,
        })));
        match sync.resume(Some(OfflineArg::Enumerate(full(items)))) {
            OfflineCoroutineState::Yielded(OfflineYield::WantsPush { .. }) => {}
            state => panic!("expected WantsPush, got {state:?}"),
        }
        let writes = match sync.resume(Some(OfflineArg::Push(results))) {
            OfflineCoroutineState::Yielded(OfflineYield::WantsWrite(writes)) => writes,
            state => panic!("expected WantsWrite, got {state:?}"),
        };
        let report = match sync.resume(Some(OfflineArg::Write)) {
            OfflineCoroutineState::Complete(Ok(report)) => report,
            state => panic!("expected Complete(Ok), got {state:?}"),
        };
        (writes, report)
    }

    /// Finds the placement an UpsertPlacement op writes for `handle`, if any.
    fn upserted<'a>(writes: &'a [OfflineWriteOp], handle: &str) -> Option<&'a OfflinePlacement> {
        writes.iter().find_map(|w| match w {
            OfflineWriteOp::UpsertPlacement(p) if p.handle.as_str() == handle => Some(p),
            _ => None,
        })
    }

    #[test]
    fn rejected_flag_push_keeps_dirty() {
        let mut local = synced("1", &[]);
        local.flags = OfflineFlags::from_iter(["flagged"]);
        local.status = OfflineStatus::Dirty;

        let mut sync = OfflineSync::new("inbox", OfflineSyncOptions::default());
        let results = vec![OfflinePushResult {
            handle: OfflineHandle::from("1"),
            outcome: OfflinePushOutcome::Rejected,
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
        local.flags = OfflineFlags::from_iter(["flagged"]);
        local.status = OfflineStatus::Dirty;

        let mut sync = OfflineSync::new("inbox", OfflineSyncOptions::default());
        let results = vec![OfflinePushResult {
            handle: OfflineHandle::from("1"),
            outcome: OfflinePushOutcome::Accepted,
            assigned: None,
            revision: None,
        }];
        let (writes, _report) = drive_push(&mut sync, vec![local], vec![remote("1", &[])], results);

        let rebased = upserted(&writes, "1").expect("an accepted flag push rebases the placement");
        assert_eq!(rebased.status, OfflineStatus::Clean);
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
        one.flags = OfflineFlags::from_iter(["flagged"]);
        one.status = OfflineStatus::Dirty;
        let mut two = synced("2", &[]);
        two.flags = OfflineFlags::from_iter(["flagged"]);
        two.status = OfflineStatus::Dirty;

        let mut sync = OfflineSync::new("inbox", OfflineSyncOptions::default());
        let results = vec![
            OfflinePushResult {
                handle: OfflineHandle::from("1"),
                outcome: OfflinePushOutcome::Accepted,
                assigned: None,
                revision: None,
            },
            OfflinePushResult {
                handle: OfflineHandle::from("2"),
                outcome: OfflinePushOutcome::Rejected,
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
            OfflineStatus::Clean,
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
        local.flags = OfflineFlags::from_iter(["flagged"]);
        local.status = OfflineStatus::Dirty;

        // First sync: the remote rejects the push, so nothing is rebased and
        // the placement stays dirty in storage.
        let mut first = OfflineSync::new("inbox", OfflineSyncOptions::default());
        let results = vec![OfflinePushResult {
            handle: OfflineHandle::from("1"),
            outcome: OfflinePushOutcome::Rejected,
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
        let mut second = OfflineSync::new("inbox", OfflineSyncOptions::default());
        let (pushes, _writes, report) = run(&mut second, vec![local], vec![remote("1", &[])]);
        let pushes = pushes.expect("the dirty change is pushed again");
        assert!(matches!(pushes[0], OfflineChange::SetFlags { .. }));
        assert_eq!(report.pushed, 1);
    }

    #[test]
    fn remote_flag_change_pulls() {
        let local = synced("1", &[]);
        let mut sync = OfflineSync::new("inbox", OfflineSyncOptions::default());
        let (pushes, writes, report) = run(&mut sync, vec![local], vec![remote("1", &["seen"])]);

        assert!(pushes.is_none());
        assert_eq!(report.pulled, 1);
        let OfflineWriteOp::UpsertPlacement(p) = &writes[0] else {
            panic!("expected UpsertPlacement, got {:?}", writes[0]);
        };
        assert!(p.flags.contains("seen"));
        assert_eq!(p.status, OfflineStatus::Clean);
    }

    #[test]
    fn divergent_flags_merge_element_wise() {
        // Local added "flagged", remote added "seen", from an empty base:
        // each side wins its own flag; the merged union is pushed and the
        // remote-won flag folded in locally, with no conflict.
        let mut local = synced("1", &[]);
        local.flags = OfflineFlags::from_iter(["flagged"]);
        local.status = OfflineStatus::Dirty;

        let mut sync = OfflineSync::new("inbox", OfflineSyncOptions::default());
        let (pushes, writes, report) = run(&mut sync, vec![local], vec![remote("1", &["seen"])]);

        match &pushes.expect("a push")[0] {
            OfflineChange::SetFlags { flags, .. } => {
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
                OfflineWriteOp::UpsertPlacement(p) if p.handle.as_str() == "1" => Some(p),
                _ => None,
            })
            .expect("a rebased placement");
        assert_eq!(rebased.status, OfflineStatus::Clean);
        assert!(rebased.flags.contains("flagged") && rebased.flags.contains("seen"));
        let base = rebased.base.as_ref().expect("a base");
        assert!(base.flags.contains("flagged") && base.flags.contains("seen"));
    }

    #[test]
    fn flag_removal_merges_against_concurrent_addition() {
        // Local removed the base flag "seen" while the remote added
        // "important": the local removal and the remote addition both win.
        let mut local = synced("1", &["seen"]);
        local.flags = OfflineFlags::default();
        local.status = OfflineStatus::Dirty;

        let mut sync = OfflineSync::new("inbox", OfflineSyncOptions::default());
        let (pushes, _writes, report) = run(
            &mut sync,
            vec![local],
            vec![remote("1", &["seen", "important"])],
        );

        match &pushes.expect("a push")[0] {
            OfflineChange::SetFlags { flags, .. } => {
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
        local.flags = OfflineFlags::from_iter(["seen"]);
        local.status = OfflineStatus::Dirty;

        let opts = OfflineSyncOptions {
            push: false,
            full: false,
        };
        let mut sync = OfflineSync::new("inbox", opts);
        let (pushes, _writes, report) = run(&mut sync, vec![local], vec![remote("1", &[])]);

        assert!(pushes.is_none(), "read-only source must not push");
        assert_eq!(report.pushed, 0);
    }

    #[test]
    fn delta_vanished_drops() {
        let local = synced("1", &["seen"]);
        let mut sync = OfflineSync::new("inbox", OfflineSyncOptions::default());
        let snapshot = delta(vec![], vec![OfflineHandle::from("1")]);
        let (pushes, writes, report) = run_snapshot(&mut sync, vec![local], snapshot);

        assert!(pushes.is_none());
        assert_eq!(report.pulled, 1);
        assert!(
            matches!(&writes[0], OfflineWriteOp::DropPlacement { handle, .. } if handle.as_str() == "1"),
            "vanished placement dropped, got {:?}",
            writes[0],
        );
    }

    #[test]
    fn delta_pull_add() {
        let mut sync = OfflineSync::new("inbox", OfflineSyncOptions::default());
        let snapshot = delta(vec![remote("9", &["seen"])], vec![]);
        let (pushes, writes, report) = run_snapshot(&mut sync, vec![], snapshot);

        assert!(pushes.is_none());
        assert_eq!(report.pulled, 1);
        let OfflineWriteOp::UpsertPlacement(p) = &writes[0] else {
            panic!("expected UpsertPlacement, got {:?}", writes[0]);
        };
        assert_eq!(p.handle.as_str(), "9");
        assert_eq!(p.level, OfflineLevel::Probed);
    }

    #[test]
    fn delta_leaves_unlisted_untouched() {
        let one = synced("1", &[]);
        let two = synced("2", &[]);
        let mut sync = OfflineSync::new("inbox", OfflineSyncOptions::default());
        // only "2" changed upstream; "1" is unlisted and must not be touched
        let snapshot = delta(vec![remote("2", &["seen"])], vec![]);
        let (pushes, writes, report) = run_snapshot(&mut sync, vec![one, two], snapshot);

        assert!(pushes.is_none());
        assert_eq!(report.pulled, 1);
        assert_eq!(writes.len(), 2, "only the changed placement and checkpoint");
        let OfflineWriteOp::UpsertPlacement(p) = &writes[0] else {
            panic!("expected UpsertPlacement, got {:?}", writes[0]);
        };
        assert_eq!(p.handle.as_str(), "2");
        assert!(p.flags.contains("seen"));
    }

    #[test]
    fn delta_pushes_unlisted_local_dirty() {
        let mut local = synced("1", &[]);
        local.flags = OfflineFlags::from_iter(["seen"]);
        local.status = OfflineStatus::Dirty;

        let mut sync = OfflineSync::new("inbox", OfflineSyncOptions::default());
        // the dirty handle is not in the delta, but its pending push must
        // still be derived against its own base
        let snapshot = delta(vec![], vec![]);
        let (pushes, _writes, report) = run_snapshot(&mut sync, vec![local], snapshot);

        let pushes = pushes.expect("a push");
        assert!(matches!(pushes[0], OfflineChange::SetFlags { .. }));
        assert_eq!(report.pushed, 1);
    }

    // --- reconcile_flags branch coverage -------------------------------

    #[test]
    fn unchanged_flags_is_noop() {
        // local == base == remote: nothing to pull or push, no placement write.
        let local = synced("1", &["seen"]);
        let mut sync = OfflineSync::new("inbox", OfflineSyncOptions::default());
        let (pushes, writes, report) = run(&mut sync, vec![local], vec![remote("1", &["seen"])]);

        assert!(pushes.is_none());
        assert_eq!(report, OfflineSyncReport::default(), "a no-op sync");
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
        local.flags = OfflineFlags::from_iter(["flagged"]);
        local.status = OfflineStatus::Dirty;

        let mut sync = OfflineSync::new("inbox", OfflineSyncOptions::default());
        let (pushes, writes, report) = run(&mut sync, vec![local], vec![remote("1", &["flagged"])]);

        assert!(pushes.is_none(), "no push when both reached the same flags");
        assert_eq!(report.conflicts, 0);
        let rebased = upserted(&writes, "1").expect("a converging rebase");
        assert_eq!(rebased.status, OfflineStatus::Clean);
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

        let mut sync = OfflineSync::new("inbox", OfflineSyncOptions::default());
        let (pushes, writes, report) = run(&mut sync, vec![local], vec![remote("1", &["seen"])]);

        assert!(pushes.is_none());
        assert_eq!(report.pulled, 1);
        let pulled = upserted(&writes, "1").expect("a converged placement");
        assert_eq!(pulled.status, OfflineStatus::Clean);
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
        placement.status = OfflineStatus::Conflict;
        placement.conflict_revision = Some("r2".into());

        let mut sync = OfflineSync::new("inbox", OfflineSyncOptions::default());
        let mut item = remote_rev("1", "r2");
        item.flags = OfflineFlags::from_iter(["seen"]);
        let (pushes, writes, report) = run(&mut sync, vec![placement], vec![item]);

        assert!(pushes.is_none());
        assert_eq!(report.pulled, 1);
        let pulled = upserted(&writes, "1").expect("a flag pull");
        assert_eq!(
            pulled.status,
            OfflineStatus::Conflict,
            "the conflict survives"
        );
        assert!(pulled.flags.contains("seen"));
        assert_eq!(
            pulled.object,
            Some(OfflineHash::from("h2")),
            "the edit survives"
        );
    }

    #[test]
    fn read_only_still_pulls_remote_changes() {
        // push=false blocks the push direction only; remote-won changes are
        // still pulled.
        let local = synced("1", &[]);
        let opts = OfflineSyncOptions {
            push: false,
            full: false,
        };
        let mut sync = OfflineSync::new("inbox", opts);
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

    // --- membership (tombstone / remote delete) coverage ---------------

    #[test]
    fn accepted_delete_drops_tombstone() {
        // A tombstone present on both sides pushes a Remove; once the remote
        // accepts it, the placement is dropped.
        let mut local = synced("1", &["seen"]);
        local.status = OfflineStatus::Tombstone;

        let mut sync = OfflineSync::new("inbox", OfflineSyncOptions::default());
        let results = vec![OfflinePushResult {
            handle: OfflineHandle::from("1"),
            outcome: OfflinePushOutcome::Accepted,
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
                |w| matches!(w, OfflineWriteOp::DropPlacement { handle, .. } if handle.as_str() == "1")
            ),
            "an accepted delete drops the tombstone: {writes:?}",
        );
    }

    #[test]
    fn rejected_delete_keeps_tombstone() {
        // A delete the remote refuses (e.g. no trash to move into) must keep
        // the tombstone, not drop a message the server still has.
        let mut local = synced("1", &["seen"]);
        local.status = OfflineStatus::Tombstone;

        let mut sync = OfflineSync::new("inbox", OfflineSyncOptions::default());
        let results = vec![OfflinePushResult {
            handle: OfflineHandle::from("1"),
            outcome: OfflinePushOutcome::Rejected,
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
                |w| matches!(w, OfflineWriteOp::DropPlacement { handle, .. } if handle.as_str() == "1")
            ),
            "a rejected delete must not drop the tombstone: {writes:?}",
        );
    }

    #[test]
    fn local_delete_gone_remote_just_drops() {
        // A tombstone whose message already vanished upstream needs no push.
        let mut local = synced("1", &["seen"]);
        local.status = OfflineStatus::Tombstone;

        let mut sync = OfflineSync::new("inbox", OfflineSyncOptions::default());
        let (pushes, writes, report) = run(&mut sync, vec![local], vec![]);

        assert!(pushes.is_none());
        assert_eq!(report.pushed, 0);
        assert!(
            writes.iter().any(
                |w| matches!(w, OfflineWriteOp::DropPlacement { handle, .. } if handle.as_str() == "1")
            ),
            "the tombstone is dropped without a push: {writes:?}",
        );
    }

    #[test]
    fn remote_delete_in_full_drops() {
        // A based placement absent from a complete snapshot was deleted
        // upstream: drop it and count a pull.
        let local = synced("1", &["seen"]);
        let mut sync = OfflineSync::new("inbox", OfflineSyncOptions::default());
        let (pushes, writes, report) = run(&mut sync, vec![local], vec![]);

        assert!(pushes.is_none());
        assert_eq!(report.pulled, 1);
        assert!(
            writes.iter().any(
                |w| matches!(w, OfflineWriteOp::DropPlacement { handle, .. } if handle.as_str() == "1")
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
        local.status = OfflineStatus::Dirty;

        let mut sync = OfflineSync::new("inbox", OfflineSyncOptions::default());
        let (pushes, writes, report) = run(&mut sync, vec![local], vec![]);

        assert!(pushes.is_none());
        assert_eq!(report, OfflineSyncReport::default());
        assert!(
            upserted(&writes, "1").is_none()
                && !writes.iter().any(
                    |w| matches!(w, OfflineWriteOp::DropPlacement { handle, .. } if handle.as_str() == "1")
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
        local.flags = OfflineFlags::from_iter(["seen"]);
        let mut sync = OfflineSync::new("inbox", OfflineSyncOptions::default());
        let (pushes, _writes, report) = run(&mut sync, vec![local], vec![]);

        match &pushes.expect("a push")[0] {
            OfflineChange::Add { origin, flags, .. } => {
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
        local.object = Some(OfflineHash::from("h-1"));
        let mut sync = OfflineSync::new("inbox", OfflineSyncOptions::default());
        let results = vec![OfflinePushResult {
            handle: OfflineHandle::from("tmp-1"),
            outcome: OfflinePushOutcome::Accepted,
            assigned: Some(OfflineHandle::from("42")),
            revision: Some("r1".into()),
        }];
        let (writes, _report) = drive_push(&mut sync, vec![local], vec![], results);

        assert!(
            writes.iter().any(
                |w| matches!(w, OfflineWriteOp::DropPlacement { handle, .. } if handle.as_str() == "tmp-1")
            ),
            "the placeholder is dropped: {writes:?}",
        );
        let real =
            upserted(&writes, "42").expect("the placement is rekeyed to the assigned handle");
        assert_eq!(real.status, OfflineStatus::Clean);
        assert!(real.origin.is_none());
        let base = real.base.as_ref().expect("a base");
        assert_eq!(base.revision.as_deref(), Some("r1"));
        assert_eq!(base.object, Some(OfflineHash::from("h-1")));
    }

    #[test]
    fn rejected_create_keeps_placeholder() {
        // A refused add keeps the placeholder for the next retry: no drop and
        // no rekey.
        let local = created("tmp-1");
        let mut sync = OfflineSync::new("inbox", OfflineSyncOptions::default());
        let results = vec![OfflinePushResult {
            handle: OfflineHandle::from("tmp-1"),
            outcome: OfflinePushOutcome::Rejected,
            assigned: None,
            revision: None,
        }];
        let (writes, report) = drive_push(&mut sync, vec![local], vec![], results);

        assert_eq!(report.rejected, 1);
        assert!(
            !writes
                .iter()
                .any(|w| matches!(w, OfflineWriteOp::DropPlacement { .. })),
            "a rejected create must not drop the placeholder: {writes:?}",
        );
        assert!(upserted(&writes, "tmp-1").is_none());
    }

    #[test]
    fn move_pushes_remove_with_target() {
        // A tombstone carrying an origin is a move: it pushes a Remove naming
        // the destination, so the consumer issues a UID MOVE.
        let mut local = synced("1", &["seen"]);
        local.status = OfflineStatus::Tombstone;
        local.origin = Some(OfflineOrigin {
            collection: "archive".into(),
            handle: OfflineHandle::from("1"),
        });

        let mut sync = OfflineSync::new("inbox", OfflineSyncOptions::default());
        let (pushes, _writes, report) = run(&mut sync, vec![local], vec![remote("1", &["seen"])]);

        match &pushes.expect("a push")[0] {
            OfflineChange::Remove { to: Some(to), .. } => assert_eq!(to.as_str(), "archive"),
            other => panic!("expected a move Remove, got {other:?}"),
        }
        assert_eq!(report.pushed, 1);
    }

    #[test]
    fn accepted_create_without_assigned_drops_placeholder() {
        // A copy whose push is accepted with no assigned handle (no UIDPLUS)
        // drops the placeholder: the next enumerate re-adds the real handle.
        let local = created("tmp-1");
        let mut sync = OfflineSync::new("inbox", OfflineSyncOptions::default());
        let results = vec![OfflinePushResult {
            handle: OfflineHandle::from("tmp-1"),
            outcome: OfflinePushOutcome::Accepted,
            assigned: None,
            revision: None,
        }];
        let (writes, _report) = drive_push(&mut sync, vec![local], vec![], results);

        assert!(
            writes.iter().any(
                |w| matches!(w, OfflineWriteOp::DropPlacement { handle, .. } if handle.as_str() == "tmp-1")
            ),
            "the placeholder is dropped once the copy lands: {writes:?}",
        );
        assert!(upserted(&writes, "tmp-1").is_none());
    }

    #[test]
    fn full_sync_ignores_checkpoint() {
        // A full sync drops the stored checkpoint, so the enumerate is asked
        // for the whole remote (cursor None) rather than a delta.
        let mut sync = OfflineSync::new(
            "inbox",
            OfflineSyncOptions {
                push: true,
                full: true,
            },
        );
        let _ = sync.resume(None);
        let loaded = OfflineLoaded {
            placements: Vec::new(),
            checkpoint: Some(OfflineCheckpoint(b"cp".to_vec())),
        };
        match sync.resume(Some(OfflineArg::Load(loaded))) {
            OfflineCoroutineState::Yielded(OfflineYield::WantsEnumerate { cursor, .. }) => {
                assert!(cursor.is_none(), "a full sync must ignore the checkpoint");
            }
            state => panic!("expected WantsEnumerate, got {state:?}"),
        }
    }

    /// A synced placement carrying a staged content edit: the body points
    /// at "h2" while the base pins "h1" at revision "r1".
    fn edited(handle: &str) -> OfflinePlacement {
        let mut placement = synced(handle, &[]);
        placement.status = OfflineStatus::Dirty;
        placement.object = Some(OfflineHash::from("h2"));
        placement.level = OfflineLevel::Full;
        let base = placement.base.as_mut().expect("a base");
        base.revision = Some("r1".into());
        base.object = Some(OfflineHash::from("h1"));
        placement
    }

    /// A remote item at the given content revision.
    fn remote_rev(handle: &str, revision: &str) -> OfflineRemoteItem {
        let mut item = remote(handle, &[]);
        item.revision = Some(revision.into());
        item
    }

    #[test]
    fn local_content_edit_pushes_update_and_rebases() {
        // A staged edit against an unchanged remote pushes an Update gated
        // on the base revision; once accepted, the base pins the pushed
        // body and the revision the remote reported.
        let mut sync = OfflineSync::new("inbox", OfflineSyncOptions::default());
        let _ = sync.resume(None);
        let _ = sync.resume(Some(OfflineArg::Load(OfflineLoaded {
            placements: vec![edited("1")],
            checkpoint: None,
        })));

        let pushes = match sync.resume(Some(OfflineArg::Enumerate(full(vec![remote_rev(
            "1", "r1",
        )])))) {
            OfflineCoroutineState::Yielded(OfflineYield::WantsPush { changes, .. }) => changes,
            state => panic!("expected WantsPush, got {state:?}"),
        };
        match &pushes[0] {
            OfflineChange::Update {
                handle,
                object,
                if_match,
            } => {
                assert_eq!(handle.as_str(), "1");
                assert_eq!(object, &OfflineHash::from("h2"));
                assert_eq!(if_match.as_deref(), Some("r1"));
            }
            other => panic!("expected an Update push, got {other:?}"),
        }

        let results = vec![OfflinePushResult {
            handle: OfflineHandle::from("1"),
            outcome: OfflinePushOutcome::Accepted,
            assigned: None,
            revision: Some("r2".into()),
        }];
        let writes = match sync.resume(Some(OfflineArg::Push(results))) {
            OfflineCoroutineState::Yielded(OfflineYield::WantsWrite(writes)) => writes,
            state => panic!("expected WantsWrite, got {state:?}"),
        };

        let rebased = upserted(&writes, "1").expect("a rebased placement");
        assert_eq!(rebased.status, OfflineStatus::Clean);
        let base = rebased.base.as_ref().expect("a base");
        assert_eq!(base.revision.as_deref(), Some("r2"));
        assert_eq!(base.object, Some(OfflineHash::from("h2")));
    }

    #[test]
    fn content_rebase_defers_a_riding_flag_edit() {
        // A placement both content-dirty and flag-dirty pushes the content;
        // the rebase keeps the base flags as they were and the placement
        // dirty, so the flag push derives on the next sync instead of
        // being silently marked synced.
        let mut placement = edited("1");
        placement.flags = OfflineFlags::from_iter(["seen"]);

        let mut sync = OfflineSync::new("inbox", OfflineSyncOptions::default());
        let results = vec![OfflinePushResult {
            handle: OfflineHandle::from("1"),
            outcome: OfflinePushOutcome::Accepted,
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
            OfflineStatus::Dirty,
            "the flag edit stays pending"
        );
        let base = rebased.base.as_ref().expect("a base");
        assert!(!base.flags.contains("seen"), "base flags stay as synced");
        assert_eq!(base.object, Some(OfflineHash::from("h2")));
    }

    #[test]
    fn remote_content_change_refreshes_the_stale_body() {
        // A remote edit of a clean placement drops the stale body and
        // rebases the revision; the next upgrade refetches on demand.
        let mut placement = synced("1", &[]);
        placement.object = Some(OfflineHash::from("h1"));
        placement.level = OfflineLevel::Full;
        let base = placement.base.as_mut().expect("a base");
        base.revision = Some("r1".into());
        base.object = Some(OfflineHash::from("h1"));

        let mut sync = OfflineSync::new("inbox", OfflineSyncOptions::default());
        let (pushes, writes, report) = run(&mut sync, vec![placement], vec![remote_rev("1", "r2")]);

        assert!(pushes.is_none(), "a refresh pushes nothing");
        assert_eq!(report.refreshed, 1);
        let refreshed = upserted(&writes, "1").expect("a refreshed placement");
        assert_eq!(refreshed.object, None, "the stale body is dropped");
        assert_eq!(
            refreshed.level,
            OfflineLevel::Probed,
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
        let mut sync = OfflineSync::new("inbox", OfflineSyncOptions::default());
        let (pushes, writes, report) =
            run(&mut sync, vec![edited("1")], vec![remote_rev("1", "r2")]);

        assert!(pushes.is_none());
        assert_eq!(report.conflicts, 1);
        assert_eq!(report.refreshed, 0);
        let conflicted = upserted(&writes, "1").expect("a conflicted placement");
        assert_eq!(conflicted.status, OfflineStatus::Conflict);
        assert_eq!(conflicted.conflict_revision.as_deref(), Some("r2"));
        assert_eq!(
            conflicted.object,
            Some(OfflineHash::from("h2")),
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
        placement.object = Some(OfflineHash::from("h1"));
        placement.level = OfflineLevel::Full;

        let mut sync = OfflineSync::new("inbox", OfflineSyncOptions::default());
        let (pushes, writes, report) = run(&mut sync, vec![placement], vec![remote_rev("1", "r9")]);

        assert!(pushes.is_none(), "an unresolved conflict pushes nothing");
        assert_eq!(report.conflicts, 1);
        let conflicted = upserted(&writes, "1").expect("a conflicted placement");
        assert_eq!(conflicted.status, OfflineStatus::Conflict);
        assert_eq!(conflicted.conflict_revision.as_deref(), Some("r9"));
        assert_eq!(
            conflicted.object,
            Some(OfflineHash::from("h1")),
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

        let mut sync = OfflineSync::new("inbox", OfflineSyncOptions::default());
        let (pushes, writes, report) =
            run(&mut sync, vec![placement], vec![remote("1", &["seen"])]);

        assert!(pushes.is_none());
        assert_eq!(report.conflicts, 0);
        let converged = upserted(&writes, "1").expect("a converged placement");
        assert_eq!(converged.status, OfflineStatus::Clean);
        assert!(converged.base.is_some(), "it becomes based on the remote");
    }

    #[test]
    fn an_unresolved_conflict_tracks_the_latest_remote_revision() {
        // A conflicted placement is left alone, but the observed remote
        // revision follows the remote so the consumer resolves against the
        // latest content.
        let mut placement = edited("1");
        placement.status = OfflineStatus::Conflict;
        placement.conflict_revision = Some("r2".into());

        let mut sync = OfflineSync::new("inbox", OfflineSyncOptions::default());
        let (pushes, writes, report) = run(&mut sync, vec![placement], vec![remote_rev("1", "r3")]);

        assert!(pushes.is_none());
        assert_eq!(report.conflicts, 0, "no recount");
        let tracked = upserted(&writes, "1").expect("an updated placement");
        assert_eq!(tracked.status, OfflineStatus::Conflict);
        assert_eq!(tracked.conflict_revision.as_deref(), Some("r3"));
        assert_eq!(
            tracked.object,
            Some(OfflineHash::from("h2")),
            "the edit survives"
        );
    }

    #[test]
    fn a_content_conflict_still_pulls_the_remote_flag_change() {
        // Content diverged AND the remote changed flags in the same delta
        // row: the conflict mark must not eat the flag change, because a
        // delta lists it exactly once and skipping the flag merge would
        // diverge the replica for good.
        let mut sync = OfflineSync::new("inbox", OfflineSyncOptions::default());
        let mut item = remote_rev("1", "r2");
        item.flags = OfflineFlags::from_iter(["seen"]);
        let (pushes, writes, report) = run(&mut sync, vec![edited("1")], vec![item]);

        assert!(pushes.is_none());
        assert_eq!(report.conflicts, 1);
        let conflicted = writes
            .iter()
            .rev()
            .find_map(|w| match w {
                OfflineWriteOp::UpsertPlacement(p) if p.handle.as_str() == "1" => Some(p),
                _ => None,
            })
            .expect("a conflict write");
        assert_eq!(conflicted.status, OfflineStatus::Conflict);
        assert_eq!(conflicted.conflict_revision.as_deref(), Some("r2"));
        assert!(conflicted.flags.contains("seen"), "the flag change lands");
        assert_eq!(
            conflicted.object,
            Some(OfflineHash::from("h2")),
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
        placement.status = OfflineStatus::Conflict;
        placement.conflict_revision = Some("r2".into());

        let mut sync = OfflineSync::new("inbox", OfflineSyncOptions::default());
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
        placement.status = OfflineStatus::Tombstone;

        let mut sync = OfflineSync::new("inbox", OfflineSyncOptions::default());
        let (pushes, writes, report) = run(&mut sync, vec![placement], vec![remote_rev("1", "r2")]);

        assert!(pushes.is_none(), "the delete is not pushed");
        assert_eq!(report.pulled, 1);
        let resurrected = upserted(&writes, "1").expect("a resurrected placement");
        assert_eq!(resurrected.status, OfflineStatus::Clean);
        let base = resurrected.base.as_ref().expect("a base");
        assert_eq!(base.revision.as_deref(), Some("r2"));
    }

    #[test]
    fn move_with_staged_edit_pushes_the_edit_before_the_move() {
        // A moved placement carrying a staged edit pushes the Update
        // first, so the relocated member holds the edited body; the
        // tombstone survives the rebase and the move derives next sync.
        let mut placement = edited("1");
        placement.status = OfflineStatus::Tombstone;
        placement.origin = Some(OfflineOrigin {
            collection: "archive".into(),
            handle: OfflineHandle::from("1"),
        });

        let mut first = OfflineSync::new("inbox", OfflineSyncOptions::default());
        let _ = first.resume(None);
        let _ = first.resume(Some(OfflineArg::Load(OfflineLoaded {
            placements: vec![placement],
            checkpoint: None,
        })));
        let pushes = match first.resume(Some(OfflineArg::Enumerate(full(vec![remote_rev(
            "1", "r1",
        )])))) {
            OfflineCoroutineState::Yielded(OfflineYield::WantsPush { changes, .. }) => changes,
            state => panic!("expected WantsPush, got {state:?}"),
        };
        match &pushes[0] {
            OfflineChange::Update {
                object, if_match, ..
            } => {
                assert_eq!(object, &OfflineHash::from("h2"), "the edit pushes first");
                assert_eq!(if_match.as_deref(), Some("r1"));
            }
            other => panic!("expected an Update push, got {other:?}"),
        }

        let results = vec![OfflinePushResult {
            handle: OfflineHandle::from("1"),
            outcome: OfflinePushOutcome::Accepted,
            assigned: None,
            revision: Some("r2".into()),
        }];
        let writes = match first.resume(Some(OfflineArg::Push(results))) {
            OfflineCoroutineState::Yielded(OfflineYield::WantsWrite(writes)) => writes,
            state => panic!("expected WantsWrite, got {state:?}"),
        };
        let rebased = upserted(&writes, "1").expect("a rebased placement");
        assert_eq!(
            rebased.status,
            OfflineStatus::Tombstone,
            "the move stays pending"
        );
        let base = rebased.base.as_ref().expect("a base");
        assert_eq!(base.revision.as_deref(), Some("r2"));
        assert_eq!(base.object, Some(OfflineHash::from("h2")));

        // second sync: the edit landed, so the move itself derives now,
        // gated on the pushed revision
        let mut second = OfflineSync::new("inbox", OfflineSyncOptions::default());
        let (pushes, _writes, _report) = run(
            &mut second,
            vec![rebased.clone()],
            vec![remote_rev("1", "r2")],
        );
        match &pushes.expect("a push")[0] {
            OfflineChange::Remove {
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
        placement.status = OfflineStatus::Tombstone;

        let mut sync = OfflineSync::new("inbox", OfflineSyncOptions::default());
        let (pushes, _writes, _report) =
            run(&mut sync, vec![placement], vec![remote_rev("1", "r1")]);

        match &pushes.expect("a push")[0] {
            OfflineChange::Remove { if_match, .. } => assert_eq!(if_match.as_deref(), Some("r1")),
            other => panic!("expected a Remove push, got {other:?}"),
        }
    }

    #[test]
    fn remote_delete_with_staged_edit_resurrects_as_create() {
        // The remote deleted what was edited locally: the edit wins over
        // the delete (the mirror of remote-update-beats-local-delete), so
        // the placement converts to a pending create that re-uploads the
        // edited body instead of being silently dropped.
        let mut sync = OfflineSync::new("inbox", OfflineSyncOptions::default());
        let (pushes, writes, _report) = run(&mut sync, vec![edited("1")], vec![]);

        match &pushes.expect("a push")[0] {
            OfflineChange::Add { object, origin, .. } => {
                assert_eq!(object, &Some(OfflineHash::from("h2")), "the edited body");
                assert!(origin.is_none(), "an append, not a copy");
            }
            other => panic!("expected an Add push, got {other:?}"),
        }
        let resurrected = upserted(&writes, "1").expect("a resurrected placement");
        assert_eq!(resurrected.status, OfflineStatus::Created);
        assert!(resurrected.base.is_none());
        assert_eq!(resurrected.object, Some(OfflineHash::from("h2")));
    }

    #[test]
    fn remote_delete_of_a_conflicted_placement_resurrects_the_edit() {
        // The remote deleted the item while a content conflict was
        // pending: the conflict is moot (the remote side is gone), and
        // the surviving local edit wins by resurrecting as a pending
        // create rather than being dropped with the placement.
        let mut placement = edited("1");
        placement.status = OfflineStatus::Conflict;
        placement.conflict_revision = Some("r2".into());

        let mut sync = OfflineSync::new("inbox", OfflineSyncOptions::default());
        let (pushes, writes, _report) = run(&mut sync, vec![placement], vec![]);

        assert!(matches!(
            &pushes.expect("a push")[0],
            OfflineChange::Add { origin: None, .. }
        ));
        let resurrected = upserted(&writes, "1").expect("a resurrected placement");
        assert_eq!(resurrected.status, OfflineStatus::Created);
        assert_eq!(resurrected.conflict_revision, None, "the conflict is moot");
        assert_eq!(
            resurrected.object,
            Some(OfflineHash::from("h2")),
            "the edit survives"
        );
    }

    #[test]
    fn read_only_remote_delete_with_staged_edit_keeps_the_edit() {
        // Same rescue on a read-only source: no push, but the placement
        // still converts to a pending create so the edit survives for a
        // later writable sync instead of being dropped.
        let opts = OfflineSyncOptions {
            push: false,
            full: false,
        };
        let mut sync = OfflineSync::new("inbox", opts);
        let (pushes, writes, _report) = run(&mut sync, vec![edited("1")], vec![]);

        assert!(pushes.is_none());
        let resurrected = upserted(&writes, "1").expect("a resurrected placement");
        assert_eq!(resurrected.status, OfflineStatus::Created);
        assert_eq!(resurrected.object, Some(OfflineHash::from("h2")));
    }

    #[test]
    fn read_only_keeps_a_content_edit_dirty() {
        // A read-only source never pushes: the staged edit stays dirty for
        // a later writable sync.
        let mut sync = OfflineSync::new(
            "inbox",
            OfflineSyncOptions {
                push: false,
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
        local.status = OfflineStatus::Tombstone;

        let opts = OfflineSyncOptions {
            push: false,
            full: false,
        };
        let mut sync = OfflineSync::new("inbox", opts);
        let (pushes, writes, report) = run(&mut sync, vec![local], vec![remote("1", &["seen"])]);

        assert!(pushes.is_none(), "read-only source must not push");
        assert_eq!(report.pushed, 0);
        assert!(
            writes.iter().any(
                |w| matches!(w, OfflineWriteOp::DropPlacement { handle, .. } if handle.as_str() == "1")
            ),
            "the tombstone is applied locally: {writes:?}",
        );
    }

    #[test]
    fn unknown_vanished_handle_is_ignored() {
        // A delta may report a vanished handle the replica never knew
        // (removed before it was ever enumerated locally): nothing to
        // merge, nothing to write, no panic.
        let mut sync = OfflineSync::new("inbox", OfflineSyncOptions::default());
        let snapshot = delta(vec![], vec![OfflineHandle::from("ghost")]);
        let (pushes, writes, report) = run_snapshot(&mut sync, vec![], snapshot);

        assert!(pushes.is_none());
        assert_eq!(report, OfflineSyncReport::default());
        assert_eq!(writes.len(), 1, "only the checkpoint write");
        assert!(matches!(&writes[0], OfflineWriteOp::SetCheckpoint { .. }));
    }

    #[test]
    fn noop_flag_edit_rebases_clean() {
        // A flag set put back to its base value is a no-op edit: nothing
        // to push, but the placement must come out clean rather than stay
        // dirty forever.
        let mut local = synced("1", &["seen"]);
        local.status = OfflineStatus::Dirty;

        let mut sync = OfflineSync::new("inbox", OfflineSyncOptions::default());
        let (pushes, writes, report) = run(&mut sync, vec![local], vec![remote("1", &["seen"])]);

        assert!(pushes.is_none(), "nothing to push");
        assert_eq!(report, OfflineSyncReport::default());
        let cleaned = upserted(&writes, "1").expect("a cleaning rebase");
        assert_eq!(cleaned.status, OfflineStatus::Clean);
    }

    #[test]
    fn missing_arg_errors() {
        let mut sync = OfflineSync::new("inbox", OfflineSyncOptions::default());
        let _ = sync.resume(None);
        match sync.resume(None) {
            OfflineCoroutineState::Complete(Err(OfflineSyncError::MissingArg)) => {}
            state => panic!("expected MissingArg, got {state:?}"),
        }
    }

    #[test]
    fn unexpected_arg_errors() {
        let mut sync = OfflineSync::new("inbox", OfflineSyncOptions::default());
        let _ = sync.resume(None);
        match sync.resume(Some(OfflineArg::Write)) {
            OfflineCoroutineState::Complete(Err(OfflineSyncError::UnexpectedArg)) => {}
            state => panic!("expected UnexpectedArg, got {state:?}"),
        }
    }
}
