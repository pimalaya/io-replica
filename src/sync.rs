//! I/O-free coroutine to reconcile a collection with its remote.
//!
//! The load-bearing verb. It loads local state, enumerates the remote
//! delta, then runs a three-way merge of Local, Base and Remote per
//! placement: local-won changes are pushed, remote-won changes are
//! pulled, divergent changes are kept both (conflict). The merge compares
//! per-placement identities (flags and a content token), never raw bytes:
//! the complete probed spine plus the per-placement base, not a missing
//! body, is what tells deleted from not-cached.
//!
//! Backends where the content itself is mutable (an item can be edited in
//! place) carry that mutation in the content token; backends where it is
//! immutable carry only flag and membership changes. Either way the merge
//! shape is the same. Permission gating drops pushes a read-only source
//! forbids.

use core::{fmt, mem};

use alloc::{collections::BTreeMap, collections::BTreeSet, vec::Vec};

use log::{debug, trace};
use thiserror::Error;

use crate::{
    change::{Change, WriteOp},
    collection::{Checkpoint, CollectionId},
    coroutine::*,
    placement::{Base, Flags, Handle, Level, Placement, Status},
    remote::{PushOutcome, RemoteItem, RemoteSnapshot},
};

/// Tuning for one sync run: the push direction and the enumerate depth.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct OfflineSyncOptions {
    /// When false the source is treated read-only: local changes are kept
    /// dirty and never pushed (permission gating).
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
    /// Changes pushed to the remote.
    pub pushed: usize,
    /// Placements left in conflict (both sides diverged).
    pub conflicts: usize,
    /// Pushes the remote rejected on optimistic concurrency.
    pub rejected: usize,
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
    collection: CollectionId,
    opts: OfflineSyncOptions,
    local: BTreeMap<Handle, Placement>,
    checkpoint: Option<Checkpoint>,
    writes: Vec<WriteOp>,
    /// Flag pushes awaiting their outcome: the local placement to rebase
    /// once the remote confirms, keyed by handle. A rejected push leaves
    /// the placement untouched (dirty) so the next sync retries it.
    pending_rebases: BTreeMap<Handle, Placement>,
    /// Tombstone deletes awaiting their outcome: the placement is dropped
    /// only once the remote confirms the delete. A rejected push keeps the
    /// tombstone so the next sync retries, rather than dropping a message
    /// the server still has (a permanent desync under incremental sync).
    pending_drops: BTreeSet<Handle>,
    /// Pending creates awaiting their outcome, keyed by provisional handle:
    /// the staged placement to rekey to the server-assigned handle once the
    /// add is accepted. A rejected push keeps the placeholder for a retry.
    pending_creates: BTreeMap<Handle, Placement>,
    report: OfflineSyncReport,
    state: State,
}

impl OfflineSync {
    /// Creates a coroutine that reconciles `collection`.
    pub fn new(collection: impl Into<CollectionId>, opts: OfflineSyncOptions) -> Self {
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
            pending_rebases: BTreeMap::new(),
            pending_drops: BTreeSet::new(),
            pending_creates: BTreeMap::new(),
            report: OfflineSyncReport::default(),
            state: State::Start,
        }
    }

    /// Runs the three-way merge, filling `self.writes` and `self.report`
    /// and returning the pushes to send.
    fn reconcile(&mut self, snapshot: RemoteSnapshot) -> Vec<Change> {
        let RemoteSnapshot {
            items,
            vanished,
            complete,
            checkpoint,
        } = snapshot;

        let remote: BTreeMap<Handle, RemoteItem> =
            items.into_iter().map(|i| (i.handle.clone(), i)).collect();
        let vanished: BTreeSet<Handle> = vanished.into_iter().collect();

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

        self.writes.push(WriteOp::SetCheckpoint {
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
        remote: &BTreeMap<Handle, RemoteItem>,
    ) -> Vec<(Handle, Option<RemoteItem>)> {
        let handles: BTreeSet<Handle> = self.local.keys().chain(remote.keys()).cloned().collect();
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
        remote: &BTreeMap<Handle, RemoteItem>,
        vanished: &BTreeSet<Handle>,
    ) -> Vec<(Handle, Option<RemoteItem>)> {
        let dirty = self
            .local
            .iter()
            .filter(|(_, p)| p.status != Status::Clean)
            .map(|(handle, _)| handle.clone());

        let handles: BTreeSet<Handle> = remote
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
                    self.local
                        .get(&handle)
                        .and_then(|p| p.base.as_ref())
                        .filter(|base| base.present)
                        .map(|base| RemoteItem {
                            handle: handle.clone(),
                            flags: base.flags.clone(),
                        })
                };
                (handle, item)
            })
            .collect()
    }

    /// Three-way merges one handle against its remote state, writing the
    /// resolved placement and returning a push when the local side won.
    fn merge(&mut self, handle: &Handle, remote_item: Option<RemoteItem>) -> Option<Change> {
        let local = self.local.get(handle).cloned();

        let base_present = local
            .as_ref()
            .and_then(|p| p.base.as_ref())
            .map(|b| b.present)
            .unwrap_or(false);

        let local_tombstone = local
            .as_ref()
            .map(|p| p.status == Status::Tombstone)
            .unwrap_or(false);
        let local_present = local.is_some() && !local_tombstone;
        let remote_present = remote_item.is_some();

        match (local_present, base_present, remote_present) {
            // local delete or move: both knew it, we removed it here
            (false, true, true) if local_tombstone => {
                if !self.opts.push {
                    // Read-only source: apply the delete locally only.
                    self.drop(handle);
                    return None;
                }
                // Hold the drop until the remote confirms it (in PendingPush);
                // a rejected push keeps the tombstone for the next retry. A
                // move carries its destination in `origin`; a plain delete has
                // none, and the consumer routes it to trash.
                self.pending_drops.insert(handle.clone());
                self.report.pushed += 1;
                let to = local
                    .as_ref()
                    .and_then(|p| p.origin.as_ref())
                    .map(|o| o.collection.clone());
                Some(Change::Remove {
                    handle: handle.clone(),
                    to,
                })
            }
            // local delete of something already gone remote
            (false, _, false) if local_tombstone => {
                self.drop(handle);
                None
            }
            // remote delete: we had it in sync, it vanished upstream
            (true, true, false) => {
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
            // present on both: reconcile flags
            (true, _, true) => {
                let local = local.as_ref().expect("local present");
                let item = remote_item.as_ref().expect("remote present");
                let change = self.reconcile_flags(local, item)?;
                self.report.pushed += 1;
                Some(change)
            }
            // local create (no base, not upstream): a pending copy/move or
            // append. Stage the add; the rekey to the server-assigned handle
            // is held until the push reports it (in PendingPush). A plain
            // base-less placement that is not Created is left untouched.
            (true, false, false) => {
                let local = local.as_ref().expect("local present");
                if self.opts.push && local.status == Status::Created {
                    self.pending_creates.insert(handle.clone(), local.clone());
                    self.report.pushed += 1;
                    return Some(Change::Add {
                        handle: handle.clone(),
                        origin: local.origin.clone(),
                        object: None,
                    });
                }
                None
            }
            _ => None,
        }
    }

    /// Reconciles the flag sets of a placement present on both sides,
    /// returning a push when the local side won.
    fn reconcile_flags(&mut self, local: &Placement, remote: &RemoteItem) -> Option<Change> {
        let base_flags = local.base.as_ref().map(|b| b.flags.clone());

        let Some(base_flags) = base_flags else {
            // NOTE: never based but present on both: converge on remote
            self.pull_flags(local, &remote.flags);
            self.report.pulled += 1;
            return None;
        };

        let local_changed = local.flags != base_flags;
        let remote_changed = remote.flags != base_flags;

        match (local_changed, remote_changed) {
            (false, true) => {
                self.pull_flags(local, &remote.flags);
                self.report.pulled += 1;
                None
            }
            (true, false) => {
                if !self.opts.push {
                    // NOTE: read-only source, keep dirty and do not push
                    return None;
                }
                // Hold the rebase until the push is confirmed: a rejected
                // push must leave the placement dirty so the next sync
                // retries it, not rebase it onto a state the remote never
                // took (which QRESYNC would then never revisit).
                self.pending_rebases
                    .insert(local.handle.clone(), local.clone());
                Some(Change::SetFlags {
                    handle: local.handle.clone(),
                    flags: local.flags.clone(),
                })
            }
            (true, true) if local.flags == remote.flags => {
                self.rebase(local, &remote.flags);
                None
            }
            (true, true) => {
                let mut conflicted = local.clone();
                conflicted.status = Status::Conflict;
                self.writes.push(WriteOp::UpsertPlacement(conflicted));
                self.report.conflicts += 1;
                None
            }
            (false, false) => None,
        }
    }

    fn drop(&mut self, handle: &Handle) {
        self.writes.push(WriteOp::DropPlacement {
            collection: self.collection.clone(),
            handle: handle.clone(),
        });
    }

    fn pull_add(&mut self, item: &RemoteItem) {
        let placement = Placement {
            collection: self.collection.clone(),
            handle: item.handle.clone(),
            link_id: None,
            object: None,
            level: Level::Probed,
            meta: None,
            flags: item.flags.clone(),
            status: Status::Clean,
            base: Some(Base {
                flags: item.flags.clone(),
                present: true,
                etag: None,
            }),
            origin: None,
        };
        self.writes.push(WriteOp::UpsertPlacement(placement));
    }

    fn pull_flags(&mut self, local: &Placement, flags: &Flags) {
        let mut updated = local.clone();
        updated.flags = flags.clone();
        updated.status = Status::Clean;
        updated.base = Some(Base {
            flags: flags.clone(),
            present: true,
            etag: local.base.as_ref().and_then(|b| b.etag.clone()),
        });
        self.writes.push(WriteOp::UpsertPlacement(updated));
    }

    fn rebase(&mut self, local: &Placement, flags: &Flags) {
        let mut updated = local.clone();
        updated.status = Status::Clean;
        updated.base = Some(Base {
            flags: flags.clone(),
            present: true,
            etag: local.base.as_ref().and_then(|b| b.etag.clone()),
        });
        self.writes.push(WriteOp::UpsertPlacement(updated));
    }

    /// Rekeys an accepted create: drops the provisional placeholder and
    /// upserts the same placement under the server-assigned `handle`, clean
    /// and based, so the next enumerate reconciles it as already in sync.
    fn rekey_create(&mut self, placeholder: Placement, handle: Handle) {
        self.drop(&placeholder.handle);
        let mut placed = placeholder;
        placed.handle = handle;
        placed.status = Status::Clean;
        placed.origin = None;
        placed.base = Some(Base {
            flags: placed.flags.clone(),
            present: true,
            etag: None,
        });
        self.writes.push(WriteOp::UpsertPlacement(placed));
    }
}

impl OfflineCoroutine for OfflineSync {
    type Yield = OfflineYield;
    type Return = Result<OfflineSyncReport, OfflineSyncError>;

    fn resume(
        &mut self,
        arg: Option<OfflineArg>,
    ) -> OfflineCoroutineState<Self::Yield, Self::Return> {
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
                        // The remote took the change: rebase a flag push clean,
                        // drop a confirmed delete, or rekey a confirmed create
                        // to its server-assigned handle, so the local state
                        // matches what the remote now holds.
                        PushOutcome::Accepted => {
                            if let Some(placement) = self.pending_rebases.remove(&result.handle) {
                                let flags = placement.flags.clone();
                                self.rebase(&placement, &flags);
                            }
                            if self.pending_drops.remove(&result.handle) {
                                self.drop(&result.handle);
                            }
                            if let Some(placeholder) = self.pending_creates.remove(&result.handle) {
                                match result.assigned.clone() {
                                    // The remote returned the new handle: rekey.
                                    Some(assigned) => self.rekey_create(placeholder, assigned),
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
                        PushOutcome::Rejected => self.report.rejected += 1,
                    }
                }
                // Any handle the push never reported on stays pending too.
                self.pending_rebases.clear();
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
                    "sync done: {} pulled, {} pushed, {} conflicts, {} rejected",
                    self.report.pulled,
                    self.report.pushed,
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
    use alloc::vec::Vec;

    use crate::{
        placement::{LinkId, Origin},
        remote::{PushOutcome, PushResult, RemoteSnapshot},
        storage::Loaded,
        sync::*,
    };

    /// A pending create staged in "inbox", its body sourced from "sent".
    fn created(handle: &str) -> Placement {
        Placement {
            collection: "inbox".into(),
            handle: Handle::from(handle),
            link_id: None,
            object: None,
            level: Level::Probed,
            meta: None,
            flags: Flags::default(),
            status: Status::Created,
            base: None,
            origin: Some(Origin {
                collection: "sent".into(),
                handle: Handle::from("9"),
            }),
        }
    }

    fn synced(handle: &str, flags: &[&str]) -> Placement {
        Placement {
            collection: "inbox".into(),
            handle: Handle::from(handle),
            link_id: Some(LinkId::from(handle)),
            object: None,
            level: Level::Probed,
            meta: None,
            flags: Flags::from_iter(flags.iter().copied()),
            status: Status::Clean,
            base: Some(Base {
                flags: Flags::from_iter(flags.iter().copied()),
                present: true,
                etag: None,
            }),
            origin: None,
        }
    }

    fn remote(handle: &str, flags: &[&str]) -> RemoteItem {
        RemoteItem {
            handle: Handle::from(handle),
            flags: Flags::from_iter(flags.iter().copied()),
        }
    }

    fn full(items: Vec<RemoteItem>) -> RemoteSnapshot {
        RemoteSnapshot {
            items,
            vanished: Vec::new(),
            complete: true,
            checkpoint: Checkpoint(b"c1".to_vec()),
        }
    }

    fn delta(items: Vec<RemoteItem>, vanished: Vec<Handle>) -> RemoteSnapshot {
        RemoteSnapshot {
            items,
            vanished,
            complete: false,
            checkpoint: Checkpoint(b"c1".to_vec()),
        }
    }

    fn run(
        sync: &mut OfflineSync,
        local: Vec<Placement>,
        items: Vec<RemoteItem>,
    ) -> (Option<Vec<Change>>, Vec<WriteOp>, OfflineSyncReport) {
        run_snapshot(sync, local, full(items))
    }

    fn run_snapshot(
        sync: &mut OfflineSync,
        local: Vec<Placement>,
        snapshot: RemoteSnapshot,
    ) -> (Option<Vec<Change>>, Vec<WriteOp>, OfflineSyncReport) {
        let _ = sync.resume(None);
        let _ = sync.resume(Some(OfflineArg::Load(Loaded {
            placements: local,
            checkpoint: None,
        })));

        let mut pushes = None;
        let writes = match sync.resume(Some(OfflineArg::Enumerate(snapshot))) {
            OfflineCoroutineState::Yielded(OfflineYield::WantsPush { changes, .. }) => {
                pushes = Some(changes);
                let results = Vec::new();
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
        let mut sync = OfflineSync::new("inbox", OfflineSyncOptions::default());
        let (pushes, writes, report) = run(&mut sync, vec![], vec![remote("1", &["seen"])]);

        assert!(pushes.is_none());
        assert_eq!(report.pulled, 1);
        let WriteOp::UpsertPlacement(p) = &writes[0] else {
            panic!("expected UpsertPlacement, got {:?}", writes[0]);
        };
        assert_eq!(p.level, Level::Probed);
        assert!(p.flags.contains("seen"));
    }

    #[test]
    fn local_flag_change_pushes() {
        let mut local = synced("1", &[]);
        local.flags = Flags::from_iter(["seen"]);
        local.status = Status::Dirty;

        let mut sync = OfflineSync::new("inbox", OfflineSyncOptions::default());
        let (pushes, _writes, report) = run(&mut sync, vec![local], vec![remote("1", &[])]);

        let pushes = pushes.expect("a push");
        assert!(matches!(pushes[0], Change::SetFlags { .. }));
        assert_eq!(report.pushed, 1);
    }

    /// Drives a sync through its push, feeding back the given push results,
    /// and returns the storage writes the engine then stages and the report.
    fn drive_push(
        sync: &mut OfflineSync,
        local: Vec<Placement>,
        items: Vec<RemoteItem>,
        results: Vec<PushResult>,
    ) -> (Vec<WriteOp>, OfflineSyncReport) {
        let _ = sync.resume(None);
        let _ = sync.resume(Some(OfflineArg::Load(Loaded {
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
    fn upserted<'a>(writes: &'a [WriteOp], handle: &str) -> Option<&'a Placement> {
        writes.iter().find_map(|w| match w {
            WriteOp::UpsertPlacement(p) if p.handle.as_str() == handle => Some(p),
            _ => None,
        })
    }

    #[test]
    fn rejected_flag_push_keeps_dirty() {
        let mut local = synced("1", &[]);
        local.flags = Flags::from_iter(["flagged"]);
        local.status = Status::Dirty;

        let mut sync = OfflineSync::new("inbox", OfflineSyncOptions::default());
        let results = vec![PushResult {
            handle: Handle::from("1"),
            outcome: PushOutcome::Rejected,
            assigned: None,
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
        local.flags = Flags::from_iter(["flagged"]);
        local.status = Status::Dirty;

        let mut sync = OfflineSync::new("inbox", OfflineSyncOptions::default());
        let results = vec![PushResult {
            handle: Handle::from("1"),
            outcome: PushOutcome::Accepted,
            assigned: None,
        }];
        let (writes, _report) = drive_push(&mut sync, vec![local], vec![remote("1", &[])], results);

        let rebased = upserted(&writes, "1").expect("an accepted flag push rebases the placement");
        assert_eq!(rebased.status, Status::Clean);
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
        one.flags = Flags::from_iter(["flagged"]);
        one.status = Status::Dirty;
        let mut two = synced("2", &[]);
        two.flags = Flags::from_iter(["flagged"]);
        two.status = Status::Dirty;

        let mut sync = OfflineSync::new("inbox", OfflineSyncOptions::default());
        let results = vec![
            PushResult {
                handle: Handle::from("1"),
                outcome: PushOutcome::Accepted,
                assigned: None,
            },
            PushResult {
                handle: Handle::from("2"),
                outcome: PushOutcome::Rejected,
                assigned: None,
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
            Status::Clean,
        );
        assert!(
            upserted(&writes, "2").is_none(),
            "rejected handle must stay dirty: {writes:?}",
        );
        assert_eq!(report.pushed, 2, "both were attempted");
        assert_eq!(report.rejected, 1);
    }

    #[test]
    fn rejected_push_retries_on_next_sync() {
        let mut local = synced("1", &[]);
        local.flags = Flags::from_iter(["flagged"]);
        local.status = Status::Dirty;

        // First sync: the remote rejects the push, so nothing is rebased and
        // the placement stays dirty in storage.
        let mut first = OfflineSync::new("inbox", OfflineSyncOptions::default());
        let results = vec![PushResult {
            handle: Handle::from("1"),
            outcome: PushOutcome::Rejected,
            assigned: None,
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
        assert!(matches!(pushes[0], Change::SetFlags { .. }));
        assert_eq!(report.pushed, 1);
    }

    #[test]
    fn remote_flag_change_pulls() {
        let local = synced("1", &[]);
        let mut sync = OfflineSync::new("inbox", OfflineSyncOptions::default());
        let (pushes, writes, report) = run(&mut sync, vec![local], vec![remote("1", &["seen"])]);

        assert!(pushes.is_none());
        assert_eq!(report.pulled, 1);
        let WriteOp::UpsertPlacement(p) = &writes[0] else {
            panic!("expected UpsertPlacement, got {:?}", writes[0]);
        };
        assert!(p.flags.contains("seen"));
        assert_eq!(p.status, Status::Clean);
    }

    #[test]
    fn divergent_flags_conflict() {
        let mut local = synced("1", &[]);
        local.flags = Flags::from_iter(["flagged"]);
        local.status = Status::Dirty;

        let mut sync = OfflineSync::new("inbox", OfflineSyncOptions::default());
        let (pushes, writes, report) = run(&mut sync, vec![local], vec![remote("1", &["seen"])]);

        assert!(pushes.is_none());
        assert_eq!(report.conflicts, 1);
        let WriteOp::UpsertPlacement(p) = &writes[0] else {
            panic!("expected UpsertPlacement, got {:?}", writes[0]);
        };
        assert_eq!(p.status, Status::Conflict);
    }

    #[test]
    fn read_only_keeps_local_dirty() {
        let mut local = synced("1", &[]);
        local.flags = Flags::from_iter(["seen"]);
        local.status = Status::Dirty;

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
        let snapshot = delta(vec![], vec![Handle::from("1")]);
        let (pushes, writes, report) = run_snapshot(&mut sync, vec![local], snapshot);

        assert!(pushes.is_none());
        assert_eq!(report.pulled, 1);
        assert!(
            matches!(&writes[0], WriteOp::DropPlacement { handle, .. } if handle.as_str() == "1"),
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
        let WriteOp::UpsertPlacement(p) = &writes[0] else {
            panic!("expected UpsertPlacement, got {:?}", writes[0]);
        };
        assert_eq!(p.handle.as_str(), "9");
        assert_eq!(p.level, Level::Probed);
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
        let WriteOp::UpsertPlacement(p) = &writes[0] else {
            panic!("expected UpsertPlacement, got {:?}", writes[0]);
        };
        assert_eq!(p.handle.as_str(), "2");
        assert!(p.flags.contains("seen"));
    }

    #[test]
    fn delta_pushes_unlisted_local_dirty() {
        let mut local = synced("1", &[]);
        local.flags = Flags::from_iter(["seen"]);
        local.status = Status::Dirty;

        let mut sync = OfflineSync::new("inbox", OfflineSyncOptions::default());
        // the dirty handle is not in the delta, but its pending push must
        // still be derived against its own base
        let snapshot = delta(vec![], vec![]);
        let (pushes, _writes, report) = run_snapshot(&mut sync, vec![local], snapshot);

        let pushes = pushes.expect("a push");
        assert!(matches!(pushes[0], Change::SetFlags { .. }));
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
        local.flags = Flags::from_iter(["flagged"]);
        local.status = Status::Dirty;

        let mut sync = OfflineSync::new("inbox", OfflineSyncOptions::default());
        let (pushes, writes, report) = run(&mut sync, vec![local], vec![remote("1", &["flagged"])]);

        assert!(pushes.is_none(), "no push when both reached the same flags");
        assert_eq!(report.conflicts, 0);
        let rebased = upserted(&writes, "1").expect("a converging rebase");
        assert_eq!(rebased.status, Status::Clean);
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
        assert_eq!(pulled.status, Status::Clean);
        assert!(pulled.flags.contains("seen"));
        assert!(!pulled.flags.contains("flagged"), "remote flags win");
    }

    #[test]
    fn conflict_keeps_local_flags_and_base() {
        // Divergent change on both sides: the placement is flagged Conflict
        // but keeps the local flags and its original base, so it can be
        // re-resolved later rather than silently losing either side.
        let mut local = synced("1", &[]);
        local.flags = Flags::from_iter(["flagged"]);
        local.status = Status::Dirty;

        let mut sync = OfflineSync::new("inbox", OfflineSyncOptions::default());
        let (pushes, writes, report) = run(&mut sync, vec![local], vec![remote("1", &["seen"])]);

        assert!(pushes.is_none());
        assert_eq!(report.conflicts, 1);
        let conflicted = upserted(&writes, "1").expect("a conflict write");
        assert_eq!(conflicted.status, Status::Conflict);
        assert!(conflicted.flags.contains("flagged"), "local flags kept");
        assert!(!conflicted.flags.contains("seen"));
        assert!(
            conflicted.base.as_ref().expect("a base").flags.0.is_empty(),
            "the base is untouched by a conflict",
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
        local.status = Status::Tombstone;

        let mut sync = OfflineSync::new("inbox", OfflineSyncOptions::default());
        let results = vec![PushResult {
            handle: Handle::from("1"),
            outcome: PushOutcome::Accepted,
            assigned: None,
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
                |w| matches!(w, WriteOp::DropPlacement { handle, .. } if handle.as_str() == "1")
            ),
            "an accepted delete drops the tombstone: {writes:?}",
        );
    }

    #[test]
    fn rejected_delete_keeps_tombstone() {
        // A delete the remote refuses (e.g. no trash to move into) must keep
        // the tombstone, not drop a message the server still has.
        let mut local = synced("1", &["seen"]);
        local.status = Status::Tombstone;

        let mut sync = OfflineSync::new("inbox", OfflineSyncOptions::default());
        let results = vec![PushResult {
            handle: Handle::from("1"),
            outcome: PushOutcome::Rejected,
            assigned: None,
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
                |w| matches!(w, WriteOp::DropPlacement { handle, .. } if handle.as_str() == "1")
            ),
            "a rejected delete must not drop the tombstone: {writes:?}",
        );
    }

    #[test]
    fn local_delete_gone_remote_just_drops() {
        // A tombstone whose message already vanished upstream needs no push.
        let mut local = synced("1", &["seen"]);
        local.status = Status::Tombstone;

        let mut sync = OfflineSync::new("inbox", OfflineSyncOptions::default());
        let (pushes, writes, report) = run(&mut sync, vec![local], vec![]);

        assert!(pushes.is_none());
        assert_eq!(report.pushed, 0);
        assert!(
            writes.iter().any(
                |w| matches!(w, WriteOp::DropPlacement { handle, .. } if handle.as_str() == "1")
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
                |w| matches!(w, WriteOp::DropPlacement { handle, .. } if handle.as_str() == "1")
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
        local.status = Status::Dirty;

        let mut sync = OfflineSync::new("inbox", OfflineSyncOptions::default());
        let (pushes, writes, report) = run(&mut sync, vec![local], vec![]);

        assert!(pushes.is_none());
        assert_eq!(report, OfflineSyncReport::default());
        assert!(
            upserted(&writes, "1").is_none()
                && !writes.iter().any(
                    |w| matches!(w, WriteOp::DropPlacement { handle, .. } if handle.as_str() == "1")
                ),
            "an offline-created item is neither rewritten nor dropped: {writes:?}",
        );
    }

    #[test]
    fn created_placement_pushes_add() {
        // A Created placement (no base, not upstream) pushes an Add carrying
        // its origin, so the remote can copy rather than re-upload.
        let local = created("tmp-1");
        let mut sync = OfflineSync::new("inbox", OfflineSyncOptions::default());
        let (pushes, _writes, report) = run(&mut sync, vec![local], vec![]);

        let pushes = pushes.expect("a push");
        assert!(matches!(
            pushes[0],
            Change::Add {
                origin: Some(_),
                ..
            }
        ));
        assert_eq!(report.pushed, 1);
    }

    #[test]
    fn accepted_create_rekeys_to_assigned() {
        // Once the add is accepted, the provisional placeholder is dropped
        // and the placement is rekeyed clean and based under the assigned
        // handle the server returned.
        let local = created("tmp-1");
        let mut sync = OfflineSync::new("inbox", OfflineSyncOptions::default());
        let results = vec![PushResult {
            handle: Handle::from("tmp-1"),
            outcome: PushOutcome::Accepted,
            assigned: Some(Handle::from("42")),
        }];
        let (writes, _report) = drive_push(&mut sync, vec![local], vec![], results);

        assert!(
            writes.iter().any(
                |w| matches!(w, WriteOp::DropPlacement { handle, .. } if handle.as_str() == "tmp-1")
            ),
            "the placeholder is dropped: {writes:?}",
        );
        let real =
            upserted(&writes, "42").expect("the placement is rekeyed to the assigned handle");
        assert_eq!(real.status, Status::Clean);
        assert!(real.base.is_some());
        assert!(real.origin.is_none());
    }

    #[test]
    fn rejected_create_keeps_placeholder() {
        // A refused add keeps the placeholder for the next retry: no drop and
        // no rekey.
        let local = created("tmp-1");
        let mut sync = OfflineSync::new("inbox", OfflineSyncOptions::default());
        let results = vec![PushResult {
            handle: Handle::from("tmp-1"),
            outcome: PushOutcome::Rejected,
            assigned: None,
        }];
        let (writes, report) = drive_push(&mut sync, vec![local], vec![], results);

        assert_eq!(report.rejected, 1);
        assert!(
            !writes
                .iter()
                .any(|w| matches!(w, WriteOp::DropPlacement { .. })),
            "a rejected create must not drop the placeholder: {writes:?}",
        );
        assert!(upserted(&writes, "tmp-1").is_none());
    }

    #[test]
    fn move_pushes_remove_with_target() {
        // A tombstone carrying an origin is a move: it pushes a Remove naming
        // the destination, so the consumer issues a UID MOVE.
        let mut local = synced("1", &["seen"]);
        local.status = Status::Tombstone;
        local.origin = Some(Origin {
            collection: "archive".into(),
            handle: Handle::from("1"),
        });

        let mut sync = OfflineSync::new("inbox", OfflineSyncOptions::default());
        let (pushes, _writes, report) = run(&mut sync, vec![local], vec![remote("1", &["seen"])]);

        match &pushes.expect("a push")[0] {
            Change::Remove { to: Some(to), .. } => assert_eq!(to.as_str(), "archive"),
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
        let results = vec![PushResult {
            handle: Handle::from("tmp-1"),
            outcome: PushOutcome::Accepted,
            assigned: None,
        }];
        let (writes, _report) = drive_push(&mut sync, vec![local], vec![], results);

        assert!(
            writes.iter().any(
                |w| matches!(w, WriteOp::DropPlacement { handle, .. } if handle.as_str() == "tmp-1")
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
        let loaded = Loaded {
            placements: Vec::new(),
            checkpoint: Some(Checkpoint(b"cp".to_vec())),
        };
        match sync.resume(Some(OfflineArg::Load(loaded))) {
            OfflineCoroutineState::Yielded(OfflineYield::WantsEnumerate { cursor, .. }) => {
                assert!(cursor.is_none(), "a full sync must ignore the checkpoint");
            }
            state => panic!("expected WantsEnumerate, got {state:?}"),
        }
    }
}
