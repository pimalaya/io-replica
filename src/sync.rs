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

/// Whether the local side may push to the remote.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct OfflineSyncOptions {
    /// When false the source is treated read-only: local changes are kept
    /// dirty and never pushed (permission gating).
    pub push: bool,
}

impl Default for OfflineSyncOptions {
    fn default() -> Self {
        Self { push: true }
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
            // local delete: both knew it, we removed it
            (false, true, true) if local_tombstone => {
                self.drop(handle);
                if self.opts.push {
                    self.report.pushed += 1;
                    return Some(Change::Remove(handle.clone()));
                }
                None
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
            // local add (no base, not upstream): offline-created item.
            // NOTE: a create needs the content to upload; deferred to the
            // create path, left untouched here.
            (true, false, false) => None,
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
                self.rebase(local, &local.flags);
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
                self.checkpoint = loaded.checkpoint;

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
                    if result.outcome == PushOutcome::Rejected {
                        self.report.rejected += 1;
                    }
                }

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

    use crate::{placement::LinkId, remote::RemoteSnapshot, storage::Loaded, sync::*};

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

        let opts = OfflineSyncOptions { push: false };
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
}
