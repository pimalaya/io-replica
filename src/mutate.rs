//! I/O-free coroutine to mutate a collection locally, with no network.
//!
//! Loads the target placement, applies the change in memory, marks it
//! dirty or tombstone (the base is left untouched so the next sync can
//! derive the pending push), and writes it back. The remote is never
//! touched here; reconciliation is [`crate::sync`]'s job.

use core::fmt;

use alloc::{string::String, vec, vec::Vec};

use log::{debug, trace};
use thiserror::Error;

use crate::{
    change::OfflineWriteOp,
    collection::OfflineCollectionId,
    coroutine::*,
    object::OfflineObject,
    placement::{
        OfflineFlags, OfflineHandle, OfflineLevel, OfflineMeta, OfflineOrigin, OfflinePlacement,
        OfflineStatus,
    },
};

/// A local edit applied offline.
///
/// Each mutation reads one source placement in the coroutine's collection
/// and stages the resulting writes; the remote is reconciled on the next
/// sync. A copy stages a [`OfflineStatus::Created`] placement in another collection.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum OfflineMutation {
    /// Replace a placement's flag set.
    SetFlags {
        /// The placement to update.
        handle: OfflineHandle,
        /// The new flag set.
        flags: OfflineFlags,
    },
    /// Mark a placement deleted, keeping it as a tombstone until synced.
    Remove(OfflineHandle),
    /// Replace a placement's body with locally edited content: the new
    /// object is stored, the placement repointed at it and marked dirty;
    /// the base keeps the previously synced body, so the next sync derives
    /// the pending push and a content three-way merge keeps its base
    /// bytes. Editing a conflicted placement resolves it: the remote
    /// revision observed at conflict time becomes the new base revision,
    /// so the resolving push is conditioned on the remote state the
    /// resolution was merged against.
    Edit {
        /// The placement to update.
        handle: OfflineHandle,
        /// The new body's object metadata.
        object: OfflineObject,
        /// The new body bytes.
        body: Vec<u8>,
        /// The refreshed summary, when the consumer projects one; `None`
        /// keeps the cached summary.
        meta: Option<OfflineMeta>,
    },
    /// Copy a placement into `target` as a pending create that the next
    /// sync pushes with a server-side copy (no body re-upload). The source
    /// is left untouched.
    Copy {
        /// The source placement to copy.
        handle: OfflineHandle,
        /// The collection to copy it into.
        target: OfflineCollectionId,
        /// The provisional handle the copy is staged under in `target`.
        placeholder: OfflineHandle,
    },
    /// Move a placement into `target`: tombstone the source carrying its
    /// destination, so the next sync pushes one atomic server-side UID MOVE
    /// (no body re-upload, no window where it is on neither side). The target
    /// picks it up on its own next enumerate.
    Move {
        /// The source placement to move.
        handle: OfflineHandle,
        /// The collection to move it into.
        target: OfflineCollectionId,
    },
}

impl OfflineMutation {
    /// The source handle the mutation reads in the coroutine's collection.
    fn handle(&self) -> &OfflineHandle {
        match self {
            Self::SetFlags { handle, .. } => handle,
            Self::Remove(handle) => handle,
            Self::Edit { handle, .. } => handle,
            Self::Copy { handle, .. } => handle,
            Self::Move { handle, .. } => handle,
        }
    }
}

/// Failure causes during a MUTATE flow.
#[derive(Clone, Debug, Error)]
pub enum OfflineMutateError {
    /// The targeted handle has no placement in the collection.
    #[error("Offline MUTATE failed: unknown handle {0}")]
    UnknownHandle(String),
    /// The driver fed back an arg that does not match the pending yield.
    #[error("Offline MUTATE failed: unexpected coroutine arg")]
    UnexpectedArg,
    /// The driver resumed without the arg the pending yield required.
    #[error("Offline MUTATE failed: missing coroutine arg")]
    MissingArg,
}

/// I/O-free MUTATE coroutine.
pub struct OfflineMutate {
    collection: OfflineCollectionId,
    mutation: OfflineMutation,
    state: State,
}

impl OfflineMutate {
    /// Creates a coroutine that applies `mutation` to `collection`.
    pub fn new(collection: impl Into<OfflineCollectionId>, mutation: OfflineMutation) -> Self {
        let collection = collection.into();
        debug!("mutate collection {}", collection.as_str());

        Self {
            collection,
            mutation,
            state: State::Start,
        }
    }

    /// Stages the writes for the mutation given its loaded source placement.
    /// OfflineFlags and removes rewrite the source in place; a copy leaves the
    /// source untouched and stages a pending create in the target.
    fn writes(&self, mut source: OfflinePlacement) -> Vec<OfflineWriteOp> {
        match &self.mutation {
            OfflineMutation::SetFlags { flags, .. } => {
                source.flags = flags.clone();
                // NOTE: a pending create stays a create and an unresolved
                // content conflict stays a conflict (its resolution is an
                // edit); the flag change rides along either way.
                if source.status == OfflineStatus::Clean {
                    source.status = OfflineStatus::Dirty;
                }
                vec![OfflineWriteOp::UpsertPlacement(source)]
            }
            OfflineMutation::Remove(_) => {
                source.status = OfflineStatus::Tombstone;
                vec![OfflineWriteOp::UpsertPlacement(source)]
            }
            OfflineMutation::Edit {
                object, body, meta, ..
            } => {
                source.object = Some(object.hash.clone());
                source.level = OfflineLevel::Full;
                if meta.is_some() {
                    source.meta = meta.clone();
                }

                // NOTE: editing a conflict is its resolution: the base
                // adopts the remote revision the resolution was merged
                // against.
                if source.status == OfflineStatus::Conflict {
                    let revision = source.conflict_revision.take();
                    if let (Some(base), Some(revision)) = (source.base.as_mut(), revision) {
                        base.revision = Some(revision);
                    }
                }

                if source.status != OfflineStatus::Created {
                    source.status = OfflineStatus::Dirty;
                }

                vec![
                    OfflineWriteOp::StoreObject {
                        object: object.clone(),
                        body: body.clone(),
                    },
                    OfflineWriteOp::UpsertPlacement(source),
                ]
            }
            OfflineMutation::Copy {
                target,
                placeholder,
                ..
            } => {
                let create = OfflinePlacement {
                    collection: target.clone(),
                    handle: placeholder.clone(),
                    link_id: source.link_id.clone(),
                    object: source.object.clone(),
                    level: source.level,
                    meta: source.meta.clone(),
                    flags: source.flags.clone(),
                    status: OfflineStatus::Created,
                    conflict_revision: None,
                    base: None,
                    origin: Some(OfflineOrigin {
                        collection: source.collection.clone(),
                        handle: source.handle.clone(),
                    }),
                };
                vec![OfflineWriteOp::UpsertPlacement(create)]
            }
            OfflineMutation::Move { target, .. } => {
                // Tombstone the source, carrying the move destination in its
                // origin so the sync pushes a UID MOVE rather than a trash
                // delete. No target placement: it appears there on the next
                // enumerate.
                source.status = OfflineStatus::Tombstone;
                source.origin = Some(OfflineOrigin {
                    collection: target.clone(),
                    handle: source.handle.clone(),
                });
                vec![OfflineWriteOp::UpsertPlacement(source)]
            }
        }
    }
}

impl OfflineCoroutine for OfflineMutate {
    type Yield = OfflineYield;
    type Return = Result<(), OfflineMutateError>;

    fn resume(
        &mut self,
        arg: Option<OfflineArg>,
    ) -> OfflineCoroutineState<Self::Yield, Self::Return> {
        trace!("mutate: {}", self.state);

        match (&mut self.state, arg) {
            (State::Start, None) => {
                debug!("load target item from storage");
                self.state = State::PendingLoad;
                OfflineCoroutineState::Yielded(OfflineYield::WantsLoad(self.collection.clone()))
            }
            (State::PendingLoad, Some(OfflineArg::Load(loaded))) => {
                let handle = self.mutation.handle();

                let Some(placement) = loaded.placements.into_iter().find(|p| &p.handle == handle)
                else {
                    let err = OfflineMutateError::UnknownHandle(handle.as_str().into());
                    return OfflineCoroutineState::Complete(Err(err));
                };

                let ops = self.writes(placement);
                debug!(
                    "stage local change on {}, {} write(s)",
                    handle.as_str(),
                    ops.len()
                );
                trace!("writes: {ops:?}");

                self.state = State::PendingWrite;
                OfflineCoroutineState::Yielded(OfflineYield::WantsWrite(ops))
            }
            (State::PendingWrite, Some(OfflineArg::Write)) => {
                debug!("local change written");
                OfflineCoroutineState::Complete(Ok(()))
            }
            (_, Some(_)) => OfflineCoroutineState::Complete(Err(OfflineMutateError::UnexpectedArg)),
            (_, None) => OfflineCoroutineState::Complete(Err(OfflineMutateError::MissingArg)),
        }
    }
}

enum State {
    Start,
    PendingLoad,
    PendingWrite,
}

impl fmt::Display for State {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Start => f.write_str("start"),
            Self::PendingLoad => f.write_str("pending load"),
            Self::PendingWrite => f.write_str("pending write"),
        }
    }
}

#[cfg(test)]
mod tests {
    use alloc::vec;

    use crate::{
        mutate::*,
        placement::{OfflineBase, OfflineLevel, OfflineStatus},
        storage::OfflineLoaded,
    };

    fn loaded(handle: &str) -> OfflineLoaded {
        crate::testlog::init();
        OfflineLoaded {
            placements: vec![OfflinePlacement {
                collection: "inbox".into(),
                handle: OfflineHandle::from(handle),
                link_id: None,
                object: None,
                level: OfflineLevel::Meta,
                meta: None,
                flags: OfflineFlags::default(),
                conflict_revision: None,
                status: OfflineStatus::Clean,
                base: Some(OfflineBase {
                    flags: OfflineFlags::default(),
                    revision: None,
                    object: None,
                }),
                origin: None,
            }],
            checkpoint: None,
        }
    }

    #[test]
    fn set_flags_marks_dirty() {
        let mutation = OfflineMutation::SetFlags {
            handle: OfflineHandle::from("1"),
            flags: OfflineFlags::from_iter(["seen"]),
        };
        let mut mutate = OfflineMutate::new("inbox", mutation);
        let _ = mutate.resume(None);

        let ops = match mutate.resume(Some(OfflineArg::Load(loaded("1")))) {
            OfflineCoroutineState::Yielded(OfflineYield::WantsWrite(ops)) => ops,
            state => panic!("expected WantsWrite, got {state:?}"),
        };
        let OfflineWriteOp::UpsertPlacement(p) = &ops[0] else {
            panic!("expected UpsertPlacement, got {:?}", ops[0]);
        };
        assert_eq!(p.status, OfflineStatus::Dirty);
        assert!(p.flags.contains("seen"));
        assert!(p.base.is_some(), "base must be preserved for sync");
    }

    #[test]
    fn set_flags_on_a_conflicted_placement_keeps_the_conflict() {
        // The flag edit rides along; the content conflict stays unresolved
        // (its resolution is an edit), so the sync never mistakes the
        // placement for a plain dirty one.
        let mutation = OfflineMutation::SetFlags {
            handle: OfflineHandle::from("1"),
            flags: OfflineFlags::from_iter(["seen"]),
        };
        let mut mutate = OfflineMutate::new("inbox", mutation);
        let _ = mutate.resume(None);

        let mut loaded = loaded("1");
        loaded.placements[0].status = OfflineStatus::Conflict;
        loaded.placements[0].conflict_revision = Some("r2".into());

        let ops = match mutate.resume(Some(OfflineArg::Load(loaded))) {
            OfflineCoroutineState::Yielded(OfflineYield::WantsWrite(ops)) => ops,
            state => panic!("expected WantsWrite, got {state:?}"),
        };
        let OfflineWriteOp::UpsertPlacement(p) = &ops[0] else {
            panic!("expected UpsertPlacement, got {:?}", ops[0]);
        };
        assert_eq!(p.status, OfflineStatus::Conflict);
        assert_eq!(p.conflict_revision.as_deref(), Some("r2"));
        assert!(p.flags.contains("seen"));
    }

    #[test]
    fn set_flags_on_a_created_placement_stays_created() {
        // A pending create keeps its status, else the sync would never
        // push the add.
        let mutation = OfflineMutation::SetFlags {
            handle: OfflineHandle::from("1"),
            flags: OfflineFlags::from_iter(["seen"]),
        };
        let mut mutate = OfflineMutate::new("inbox", mutation);
        let _ = mutate.resume(None);

        let mut loaded = loaded("1");
        loaded.placements[0].status = OfflineStatus::Created;
        loaded.placements[0].base = None;

        let ops = match mutate.resume(Some(OfflineArg::Load(loaded))) {
            OfflineCoroutineState::Yielded(OfflineYield::WantsWrite(ops)) => ops,
            state => panic!("expected WantsWrite, got {state:?}"),
        };
        let OfflineWriteOp::UpsertPlacement(p) = &ops[0] else {
            panic!("expected UpsertPlacement, got {:?}", ops[0]);
        };
        assert_eq!(p.status, OfflineStatus::Created);
        assert!(p.flags.contains("seen"));
    }

    #[test]
    fn remove_marks_tombstone() {
        let mutation = OfflineMutation::Remove(OfflineHandle::from("1"));
        let mut mutate = OfflineMutate::new("inbox", mutation);
        let _ = mutate.resume(None);

        let ops = match mutate.resume(Some(OfflineArg::Load(loaded("1")))) {
            OfflineCoroutineState::Yielded(OfflineYield::WantsWrite(ops)) => ops,
            state => panic!("expected WantsWrite, got {state:?}"),
        };
        let OfflineWriteOp::UpsertPlacement(p) = &ops[0] else {
            panic!("expected UpsertPlacement, got {:?}", ops[0]);
        };
        assert_eq!(p.status, OfflineStatus::Tombstone);
    }

    #[test]
    fn unknown_handle_errors() {
        let mutation = OfflineMutation::Remove(OfflineHandle::from("nope"));
        let mut mutate = OfflineMutate::new("inbox", mutation);
        let _ = mutate.resume(None);

        match mutate.resume(Some(OfflineArg::Load(loaded("1")))) {
            OfflineCoroutineState::Complete(Err(OfflineMutateError::UnknownHandle(h))) => {
                assert_eq!(h, "nope");
            }
            state => panic!("expected UnknownHandle, got {state:?}"),
        }
    }

    #[test]
    fn write_completes() {
        let mutation = OfflineMutation::Remove(OfflineHandle::from("1"));
        let mut mutate = OfflineMutate::new("inbox", mutation);
        let _ = mutate.resume(None);
        let _ = mutate.resume(Some(OfflineArg::Load(loaded("1"))));

        match mutate.resume(Some(OfflineArg::Write)) {
            OfflineCoroutineState::Complete(Ok(())) => {}
            state => panic!("expected Complete(Ok), got {state:?}"),
        }
    }

    #[test]
    fn missing_arg_errors() {
        let mutation = OfflineMutation::Remove(OfflineHandle::from("1"));
        let mut mutate = OfflineMutate::new("inbox", mutation);
        let _ = mutate.resume(None);

        match mutate.resume(None) {
            OfflineCoroutineState::Complete(Err(OfflineMutateError::MissingArg)) => {}
            state => panic!("expected MissingArg, got {state:?}"),
        }
    }

    #[test]
    fn unexpected_arg_errors() {
        let mutation = OfflineMutation::Remove(OfflineHandle::from("1"));
        let mut mutate = OfflineMutate::new("inbox", mutation);
        let _ = mutate.resume(None);

        match mutate.resume(Some(OfflineArg::Write)) {
            OfflineCoroutineState::Complete(Err(OfflineMutateError::UnexpectedArg)) => {}
            state => panic!("expected UnexpectedArg, got {state:?}"),
        }
    }

    #[test]
    fn edit_stages_a_dirty_body() {
        // An edit stores the new object, repoints the placement at it at
        // full level and marks it dirty; the base keeps the synced state so
        // the next sync derives the push.
        use crate::object::{OfflineHash, OfflineObject};

        let mutation = OfflineMutation::Edit {
            handle: OfflineHandle::from("1"),
            object: OfflineObject {
                hash: OfflineHash::from("h2"),
                size: 4,
            },
            body: b"body".to_vec(),
            meta: None,
        };
        let mut mutate = OfflineMutate::new("inbox", mutation);
        let _ = mutate.resume(None);

        let ops = match mutate.resume(Some(OfflineArg::Load(loaded("1")))) {
            OfflineCoroutineState::Yielded(OfflineYield::WantsWrite(ops)) => ops,
            state => panic!("expected WantsWrite, got {state:?}"),
        };

        assert!(
            matches!(&ops[0], OfflineWriteOp::StoreObject { object, .. } if object.hash == OfflineHash::from("h2"))
        );
        let OfflineWriteOp::UpsertPlacement(p) = &ops[1] else {
            panic!("expected UpsertPlacement, got {:?}", ops[1]);
        };
        assert_eq!(p.status, OfflineStatus::Dirty);
        assert_eq!(p.object, Some(OfflineHash::from("h2")));
        assert_eq!(p.level, OfflineLevel::Full);
        assert!(p.base.is_some(), "base must be preserved for sync");
    }

    #[test]
    fn edit_refreshes_the_projected_meta() {
        // A consumer that projects a fresh summary from the edited body
        // passes it along; the cached one is replaced.
        use crate::object::{OfflineHash, OfflineObject};

        let mutation = OfflineMutation::Edit {
            handle: OfflineHandle::from("1"),
            object: OfflineObject {
                hash: OfflineHash::from("h2"),
                size: 4,
            },
            body: b"body".to_vec(),
            meta: Some(OfflineMeta("fresh".into())),
        };
        let mut mutate = OfflineMutate::new("inbox", mutation);
        let _ = mutate.resume(None);

        let ops = match mutate.resume(Some(OfflineArg::Load(loaded("1")))) {
            OfflineCoroutineState::Yielded(OfflineYield::WantsWrite(ops)) => ops,
            state => panic!("expected WantsWrite, got {state:?}"),
        };
        let OfflineWriteOp::UpsertPlacement(p) = &ops[1] else {
            panic!("expected UpsertPlacement, got {:?}", ops[1]);
        };
        assert_eq!(p.meta, Some(OfflineMeta("fresh".into())));
    }

    #[test]
    fn edit_resolves_a_conflict() {
        // Editing a conflicted placement is its resolution: the base adopts
        // the remote revision observed at conflict time, so the resolving
        // push is gated on the remote state the merge was made against.
        use crate::object::{OfflineHash, OfflineObject};

        let mutation = OfflineMutation::Edit {
            handle: OfflineHandle::from("1"),
            object: OfflineObject {
                hash: OfflineHash::from("h3"),
                size: 6,
            },
            body: b"merged".to_vec(),
            meta: None,
        };
        let mut mutate = OfflineMutate::new("inbox", mutation);
        let _ = mutate.resume(None);

        let mut loaded = loaded("1");
        loaded.placements[0].status = OfflineStatus::Conflict;
        loaded.placements[0].conflict_revision = Some("r2".into());

        let ops = match mutate.resume(Some(OfflineArg::Load(loaded))) {
            OfflineCoroutineState::Yielded(OfflineYield::WantsWrite(ops)) => ops,
            state => panic!("expected WantsWrite, got {state:?}"),
        };
        let OfflineWriteOp::UpsertPlacement(p) = &ops[1] else {
            panic!("expected UpsertPlacement, got {:?}", ops[1]);
        };

        assert_eq!(p.status, OfflineStatus::Dirty);
        assert_eq!(p.conflict_revision, None);
        let base = p.base.as_ref().expect("a base");
        assert_eq!(base.revision.as_deref(), Some("r2"));
    }

    #[test]
    fn copy_stages_created_placement_in_target() {
        // A copy leaves the source untouched and stages a Created placement
        // in the target under the placeholder, carrying its origin.
        let mutation = OfflineMutation::Copy {
            handle: OfflineHandle::from("1"),
            target: "archive".into(),
            placeholder: OfflineHandle::from("tmp-1"),
        };
        let mut mutate = OfflineMutate::new("inbox", mutation);
        let _ = mutate.resume(None);

        let ops = match mutate.resume(Some(OfflineArg::Load(loaded("1")))) {
            OfflineCoroutineState::Yielded(OfflineYield::WantsWrite(ops)) => ops,
            state => panic!("expected WantsWrite, got {state:?}"),
        };
        let OfflineWriteOp::UpsertPlacement(p) = &ops[0] else {
            panic!("expected UpsertPlacement, got {:?}", ops[0]);
        };
        assert_eq!(p.collection.as_str(), "archive");
        assert_eq!(p.handle.as_str(), "tmp-1");
        assert_eq!(p.status, OfflineStatus::Created);
        assert!(p.base.is_none());
        let origin = p.origin.as_ref().expect("the copy carries its origin");
        assert_eq!(origin.collection.as_str(), "inbox");
        assert_eq!(origin.handle.as_str(), "1");
    }

    #[test]
    fn move_tombstones_source_with_target() {
        // A move tombstones the source and records its destination in origin,
        // so the sync pushes a UID MOVE rather than a trash delete.
        let mutation = OfflineMutation::Move {
            handle: OfflineHandle::from("1"),
            target: "archive".into(),
        };
        let mut mutate = OfflineMutate::new("inbox", mutation);
        let _ = mutate.resume(None);

        let ops = match mutate.resume(Some(OfflineArg::Load(loaded("1")))) {
            OfflineCoroutineState::Yielded(OfflineYield::WantsWrite(ops)) => ops,
            state => panic!("expected WantsWrite, got {state:?}"),
        };
        let OfflineWriteOp::UpsertPlacement(p) = &ops[0] else {
            panic!("expected UpsertPlacement, got {:?}", ops[0]);
        };
        assert_eq!(
            p.collection.as_str(),
            "inbox",
            "the source row, not a target one"
        );
        assert_eq!(p.status, OfflineStatus::Tombstone);
        assert_eq!(
            p.origin
                .as_ref()
                .expect("a move target")
                .collection
                .as_str(),
            "archive",
        );
    }
}
