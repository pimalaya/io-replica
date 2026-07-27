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
    change::ReplicaWriteOp,
    collection::ReplicaCollectionId,
    coroutine::*,
    object::ReplicaObject,
    placement::{
        ReplicaFlags, ReplicaHandle, ReplicaLevel, ReplicaMeta, ReplicaOrigin, ReplicaPlacement,
        ReplicaStatus,
    },
};

/// A local edit applied offline.
///
/// Each mutation reads one source placement in the coroutine's collection
/// and stages the resulting writes; the remote is reconciled on the next
/// sync. A copy stages a [`ReplicaStatus::Created`] placement in another
/// collection.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ReplicaMutation {
    /// Replace a placement's flag set.
    SetFlags {
        /// The placement to update.
        handle: ReplicaHandle,
        /// The new flag set.
        flags: ReplicaFlags,
    },
    /// Mark a placement deleted, keeping it as a tombstone until synced.
    Remove(ReplicaHandle),
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
        handle: ReplicaHandle,
        /// The new body's object metadata.
        object: ReplicaObject,
        /// The new body bytes.
        body: Vec<u8>,
        /// The refreshed summary, when the consumer projects one; `None`
        /// keeps the cached summary.
        meta: Option<ReplicaMeta>,
    },
    /// Copy a placement into `target` as a pending create that the next
    /// sync pushes with a server-side copy (no body re-upload). The source
    /// is left untouched.
    Copy {
        /// The source placement to copy.
        handle: ReplicaHandle,
        /// The collection to copy it into.
        target: ReplicaCollectionId,
        /// The provisional handle the copy is staged under in `target`.
        placeholder: ReplicaHandle,
    },
    /// Move a placement into `target`: tombstone the source carrying its
    /// destination, so the next sync pushes one atomic server-side UID MOVE
    /// (no body re-upload, no window where it is on neither side). The target
    /// picks it up on its own next enumerate.
    Move {
        /// The source placement to move.
        handle: ReplicaHandle,
        /// The collection to move it into.
        target: ReplicaCollectionId,
    },
}

impl ReplicaMutation {
    /// The source handle the mutation reads in the coroutine's collection.
    fn handle(&self) -> &ReplicaHandle {
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
pub enum ReplicaMutateError {
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
pub struct ReplicaMutate {
    collection: ReplicaCollectionId,
    mutation: ReplicaMutation,
    state: State,
}

impl ReplicaMutate {
    /// Creates a coroutine that applies `mutation` to `collection`.
    pub fn new(collection: impl Into<ReplicaCollectionId>, mutation: ReplicaMutation) -> Self {
        let collection = collection.into();
        debug!("mutate collection {}", collection.as_str());

        Self {
            collection,
            mutation,
            state: State::Start,
        }
    }

    /// Stages the writes for the mutation given its loaded source placement.
    /// ReplicaFlags and removes rewrite the source in place; a copy leaves the
    /// source untouched and stages a pending create in the target.
    fn writes(&self, mut source: ReplicaPlacement) -> Vec<ReplicaWriteOp> {
        match &self.mutation {
            ReplicaMutation::SetFlags { flags, .. } => {
                source.flags = flags.clone();
                // NOTE: a pending create stays a create and an unresolved
                // content conflict stays a conflict (its resolution is an
                // edit); the flag change rides along either way.
                if source.status == ReplicaStatus::Clean {
                    source.status = ReplicaStatus::Dirty;
                }
                vec![ReplicaWriteOp::UpsertPlacement(source)]
            }
            ReplicaMutation::Remove(_) => {
                source.status = ReplicaStatus::Tombstone;
                vec![ReplicaWriteOp::UpsertPlacement(source)]
            }
            ReplicaMutation::Edit {
                object, body, meta, ..
            } => {
                source.object = Some(object.hash.clone());
                source.level = ReplicaLevel::Full;
                if meta.is_some() {
                    source.meta = meta.clone();
                }

                // NOTE: editing a conflict is its resolution: the base
                // adopts the remote revision the resolution was merged
                // against.
                if source.status == ReplicaStatus::Conflict {
                    let revision = source.conflict_revision.take();
                    if let (Some(base), Some(revision)) = (source.base.as_mut(), revision) {
                        base.revision = Some(revision);
                    }
                }

                if source.status != ReplicaStatus::Created {
                    source.status = ReplicaStatus::Dirty;
                }

                vec![
                    ReplicaWriteOp::StoreObject {
                        object: object.clone(),
                        body: body.clone(),
                    },
                    ReplicaWriteOp::UpsertPlacement(source),
                ]
            }
            ReplicaMutation::Copy {
                target,
                placeholder,
                ..
            } => {
                let create = ReplicaPlacement {
                    collection: target.clone(),
                    handle: placeholder.clone(),
                    link_id: source.link_id.clone(),
                    object: source.object.clone(),
                    level: source.level,
                    meta: source.meta.clone(),
                    flags: source.flags.clone(),
                    status: ReplicaStatus::Created,
                    conflict_revision: None,
                    base: None,
                    origin: Some(ReplicaOrigin {
                        collection: source.collection.clone(),
                        handle: source.handle.clone(),
                    }),
                };
                vec![ReplicaWriteOp::UpsertPlacement(create)]
            }
            ReplicaMutation::Move { target, .. } => {
                // Tombstone the source, carrying the move destination in its
                // origin so the sync pushes a UID MOVE rather than a trash
                // delete. No target placement: it appears there on the next
                // enumerate.
                source.status = ReplicaStatus::Tombstone;
                source.origin = Some(ReplicaOrigin {
                    collection: target.clone(),
                    handle: source.handle.clone(),
                });
                vec![ReplicaWriteOp::UpsertPlacement(source)]
            }
        }
    }
}

impl ReplicaCoroutine for ReplicaMutate {
    type Yield = ReplicaYield;
    type Return = Result<(), ReplicaMutateError>;

    fn resume(
        &mut self,
        arg: Option<ReplicaArg>,
    ) -> ReplicaCoroutineState<Self::Yield, Self::Return> {
        trace!("mutate: {}", self.state);

        match (&mut self.state, arg) {
            (State::Start, None) => {
                debug!("load target item from storage");
                self.state = State::PendingLoad;
                ReplicaCoroutineState::Yielded(ReplicaYield::WantsLoad(self.collection.clone()))
            }
            (State::PendingLoad, Some(ReplicaArg::Load(loaded))) => {
                let handle = self.mutation.handle();

                let Some(placement) = loaded.placements.into_iter().find(|p| &p.handle == handle)
                else {
                    let err = ReplicaMutateError::UnknownHandle(handle.as_str().into());
                    return ReplicaCoroutineState::Complete(Err(err));
                };

                let ops = self.writes(placement);
                debug!(
                    "stage local change on {}, {} write(s)",
                    handle.as_str(),
                    ops.len()
                );
                trace!("writes: {ops:?}");

                self.state = State::PendingWrite;
                ReplicaCoroutineState::Yielded(ReplicaYield::WantsWrite(ops))
            }
            (State::PendingWrite, Some(ReplicaArg::Write)) => {
                debug!("local change written");
                ReplicaCoroutineState::Complete(Ok(()))
            }
            (_, Some(_)) => ReplicaCoroutineState::Complete(Err(ReplicaMutateError::UnexpectedArg)),
            (_, None) => ReplicaCoroutineState::Complete(Err(ReplicaMutateError::MissingArg)),
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
        placement::{ReplicaBase, ReplicaLevel, ReplicaStatus},
        storage::ReplicaLoaded,
    };

    fn loaded(handle: &str) -> ReplicaLoaded {
        crate::testlog::init();
        ReplicaLoaded {
            placements: vec![ReplicaPlacement {
                collection: "inbox".into(),
                handle: ReplicaHandle::from(handle),
                link_id: None,
                object: None,
                level: ReplicaLevel::Meta,
                meta: None,
                flags: ReplicaFlags::default(),
                conflict_revision: None,
                status: ReplicaStatus::Clean,
                base: Some(ReplicaBase {
                    flags: ReplicaFlags::default(),
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
        let mutation = ReplicaMutation::SetFlags {
            handle: ReplicaHandle::from("1"),
            flags: ReplicaFlags::from_iter(["seen"]),
        };
        let mut mutate = ReplicaMutate::new("inbox", mutation);
        let _ = mutate.resume(None);

        let ops = match mutate.resume(Some(ReplicaArg::Load(loaded("1")))) {
            ReplicaCoroutineState::Yielded(ReplicaYield::WantsWrite(ops)) => ops,
            state => panic!("expected WantsWrite, got {state:?}"),
        };
        let ReplicaWriteOp::UpsertPlacement(p) = &ops[0] else {
            panic!("expected UpsertPlacement, got {:?}", ops[0]);
        };
        assert_eq!(p.status, ReplicaStatus::Dirty);
        assert!(p.flags.contains("seen"));
        assert!(p.base.is_some(), "base must be preserved for sync");
    }

    #[test]
    fn set_flags_on_a_conflicted_placement_keeps_the_conflict() {
        // The flag edit rides along; the content conflict stays unresolved
        // (its resolution is an edit), so the sync never mistakes the
        // placement for a plain dirty one.
        let mutation = ReplicaMutation::SetFlags {
            handle: ReplicaHandle::from("1"),
            flags: ReplicaFlags::from_iter(["seen"]),
        };
        let mut mutate = ReplicaMutate::new("inbox", mutation);
        let _ = mutate.resume(None);

        let mut loaded = loaded("1");
        loaded.placements[0].status = ReplicaStatus::Conflict;
        loaded.placements[0].conflict_revision = Some("r2".into());

        let ops = match mutate.resume(Some(ReplicaArg::Load(loaded))) {
            ReplicaCoroutineState::Yielded(ReplicaYield::WantsWrite(ops)) => ops,
            state => panic!("expected WantsWrite, got {state:?}"),
        };
        let ReplicaWriteOp::UpsertPlacement(p) = &ops[0] else {
            panic!("expected UpsertPlacement, got {:?}", ops[0]);
        };
        assert_eq!(p.status, ReplicaStatus::Conflict);
        assert_eq!(p.conflict_revision.as_deref(), Some("r2"));
        assert!(p.flags.contains("seen"));
    }

    #[test]
    fn set_flags_on_a_created_placement_stays_created() {
        // A pending create keeps its status, else the sync would never
        // push the add.
        let mutation = ReplicaMutation::SetFlags {
            handle: ReplicaHandle::from("1"),
            flags: ReplicaFlags::from_iter(["seen"]),
        };
        let mut mutate = ReplicaMutate::new("inbox", mutation);
        let _ = mutate.resume(None);

        let mut loaded = loaded("1");
        loaded.placements[0].status = ReplicaStatus::Created;
        loaded.placements[0].base = None;

        let ops = match mutate.resume(Some(ReplicaArg::Load(loaded))) {
            ReplicaCoroutineState::Yielded(ReplicaYield::WantsWrite(ops)) => ops,
            state => panic!("expected WantsWrite, got {state:?}"),
        };
        let ReplicaWriteOp::UpsertPlacement(p) = &ops[0] else {
            panic!("expected UpsertPlacement, got {:?}", ops[0]);
        };
        assert_eq!(p.status, ReplicaStatus::Created);
        assert!(p.flags.contains("seen"));
    }

    #[test]
    fn remove_marks_tombstone() {
        let mutation = ReplicaMutation::Remove(ReplicaHandle::from("1"));
        let mut mutate = ReplicaMutate::new("inbox", mutation);
        let _ = mutate.resume(None);

        let ops = match mutate.resume(Some(ReplicaArg::Load(loaded("1")))) {
            ReplicaCoroutineState::Yielded(ReplicaYield::WantsWrite(ops)) => ops,
            state => panic!("expected WantsWrite, got {state:?}"),
        };
        let ReplicaWriteOp::UpsertPlacement(p) = &ops[0] else {
            panic!("expected UpsertPlacement, got {:?}", ops[0]);
        };
        assert_eq!(p.status, ReplicaStatus::Tombstone);
    }

    #[test]
    fn unknown_handle_errors() {
        let mutation = ReplicaMutation::Remove(ReplicaHandle::from("nope"));
        let mut mutate = ReplicaMutate::new("inbox", mutation);
        let _ = mutate.resume(None);

        match mutate.resume(Some(ReplicaArg::Load(loaded("1")))) {
            ReplicaCoroutineState::Complete(Err(ReplicaMutateError::UnknownHandle(h))) => {
                assert_eq!(h, "nope");
            }
            state => panic!("expected UnknownHandle, got {state:?}"),
        }
    }

    #[test]
    fn write_completes() {
        let mutation = ReplicaMutation::Remove(ReplicaHandle::from("1"));
        let mut mutate = ReplicaMutate::new("inbox", mutation);
        let _ = mutate.resume(None);
        let _ = mutate.resume(Some(ReplicaArg::Load(loaded("1"))));

        match mutate.resume(Some(ReplicaArg::Write)) {
            ReplicaCoroutineState::Complete(Ok(())) => {}
            state => panic!("expected Complete(Ok), got {state:?}"),
        }
    }

    #[test]
    fn missing_arg_errors() {
        let mutation = ReplicaMutation::Remove(ReplicaHandle::from("1"));
        let mut mutate = ReplicaMutate::new("inbox", mutation);
        let _ = mutate.resume(None);

        match mutate.resume(None) {
            ReplicaCoroutineState::Complete(Err(ReplicaMutateError::MissingArg)) => {}
            state => panic!("expected MissingArg, got {state:?}"),
        }
    }

    #[test]
    fn unexpected_arg_errors() {
        let mutation = ReplicaMutation::Remove(ReplicaHandle::from("1"));
        let mut mutate = ReplicaMutate::new("inbox", mutation);
        let _ = mutate.resume(None);

        match mutate.resume(Some(ReplicaArg::Write)) {
            ReplicaCoroutineState::Complete(Err(ReplicaMutateError::UnexpectedArg)) => {}
            state => panic!("expected UnexpectedArg, got {state:?}"),
        }
    }

    #[test]
    fn edit_stages_a_dirty_body() {
        // An edit stores the new object, repoints the placement at it at
        // full level and marks it dirty; the base keeps the synced state so
        // the next sync derives the push.
        use crate::object::{ReplicaHash, ReplicaObject};

        let mutation = ReplicaMutation::Edit {
            handle: ReplicaHandle::from("1"),
            object: ReplicaObject {
                hash: ReplicaHash::from("h2"),
                size: 4,
            },
            body: b"body".to_vec(),
            meta: None,
        };
        let mut mutate = ReplicaMutate::new("inbox", mutation);
        let _ = mutate.resume(None);

        let ops = match mutate.resume(Some(ReplicaArg::Load(loaded("1")))) {
            ReplicaCoroutineState::Yielded(ReplicaYield::WantsWrite(ops)) => ops,
            state => panic!("expected WantsWrite, got {state:?}"),
        };

        assert!(
            matches!(&ops[0], ReplicaWriteOp::StoreObject { object, .. } if object.hash == ReplicaHash::from("h2"))
        );
        let ReplicaWriteOp::UpsertPlacement(p) = &ops[1] else {
            panic!("expected UpsertPlacement, got {:?}", ops[1]);
        };
        assert_eq!(p.status, ReplicaStatus::Dirty);
        assert_eq!(p.object, Some(ReplicaHash::from("h2")));
        assert_eq!(p.level, ReplicaLevel::Full);
        assert!(p.base.is_some(), "base must be preserved for sync");
    }

    #[test]
    fn edit_refreshes_the_projected_meta() {
        // A consumer that projects a fresh summary from the edited body
        // passes it along; the cached one is replaced.
        use crate::object::{ReplicaHash, ReplicaObject};

        let mutation = ReplicaMutation::Edit {
            handle: ReplicaHandle::from("1"),
            object: ReplicaObject {
                hash: ReplicaHash::from("h2"),
                size: 4,
            },
            body: b"body".to_vec(),
            meta: Some(ReplicaMeta("fresh".into())),
        };
        let mut mutate = ReplicaMutate::new("inbox", mutation);
        let _ = mutate.resume(None);

        let ops = match mutate.resume(Some(ReplicaArg::Load(loaded("1")))) {
            ReplicaCoroutineState::Yielded(ReplicaYield::WantsWrite(ops)) => ops,
            state => panic!("expected WantsWrite, got {state:?}"),
        };
        let ReplicaWriteOp::UpsertPlacement(p) = &ops[1] else {
            panic!("expected UpsertPlacement, got {:?}", ops[1]);
        };
        assert_eq!(p.meta, Some(ReplicaMeta("fresh".into())));
    }

    #[test]
    fn edit_resolves_a_conflict() {
        // Editing a conflicted placement is its resolution: the base adopts
        // the remote revision observed at conflict time, so the resolving
        // push is gated on the remote state the merge was made against.
        use crate::object::{ReplicaHash, ReplicaObject};

        let mutation = ReplicaMutation::Edit {
            handle: ReplicaHandle::from("1"),
            object: ReplicaObject {
                hash: ReplicaHash::from("h3"),
                size: 6,
            },
            body: b"merged".to_vec(),
            meta: None,
        };
        let mut mutate = ReplicaMutate::new("inbox", mutation);
        let _ = mutate.resume(None);

        let mut loaded = loaded("1");
        loaded.placements[0].status = ReplicaStatus::Conflict;
        loaded.placements[0].conflict_revision = Some("r2".into());

        let ops = match mutate.resume(Some(ReplicaArg::Load(loaded))) {
            ReplicaCoroutineState::Yielded(ReplicaYield::WantsWrite(ops)) => ops,
            state => panic!("expected WantsWrite, got {state:?}"),
        };
        let ReplicaWriteOp::UpsertPlacement(p) = &ops[1] else {
            panic!("expected UpsertPlacement, got {:?}", ops[1]);
        };

        assert_eq!(p.status, ReplicaStatus::Dirty);
        assert_eq!(p.conflict_revision, None);
        let base = p.base.as_ref().expect("a base");
        assert_eq!(base.revision.as_deref(), Some("r2"));
    }

    #[test]
    fn copy_stages_created_placement_in_target() {
        // A copy leaves the source untouched and stages a Created placement
        // in the target under the placeholder, carrying its origin.
        let mutation = ReplicaMutation::Copy {
            handle: ReplicaHandle::from("1"),
            target: "archive".into(),
            placeholder: ReplicaHandle::from("tmp-1"),
        };
        let mut mutate = ReplicaMutate::new("inbox", mutation);
        let _ = mutate.resume(None);

        let ops = match mutate.resume(Some(ReplicaArg::Load(loaded("1")))) {
            ReplicaCoroutineState::Yielded(ReplicaYield::WantsWrite(ops)) => ops,
            state => panic!("expected WantsWrite, got {state:?}"),
        };
        let ReplicaWriteOp::UpsertPlacement(p) = &ops[0] else {
            panic!("expected UpsertPlacement, got {:?}", ops[0]);
        };
        assert_eq!(p.collection.as_str(), "archive");
        assert_eq!(p.handle.as_str(), "tmp-1");
        assert_eq!(p.status, ReplicaStatus::Created);
        assert!(p.base.is_none());
        let origin = p.origin.as_ref().expect("the copy carries its origin");
        assert_eq!(origin.collection.as_str(), "inbox");
        assert_eq!(origin.handle.as_str(), "1");
    }

    #[test]
    fn move_tombstones_source_with_target() {
        // A move tombstones the source and records its destination in origin,
        // so the sync pushes a UID MOVE rather than a trash delete.
        let mutation = ReplicaMutation::Move {
            handle: ReplicaHandle::from("1"),
            target: "archive".into(),
        };
        let mut mutate = ReplicaMutate::new("inbox", mutation);
        let _ = mutate.resume(None);

        let ops = match mutate.resume(Some(ReplicaArg::Load(loaded("1")))) {
            ReplicaCoroutineState::Yielded(ReplicaYield::WantsWrite(ops)) => ops,
            state => panic!("expected WantsWrite, got {state:?}"),
        };
        let ReplicaWriteOp::UpsertPlacement(p) = &ops[0] else {
            panic!("expected UpsertPlacement, got {:?}", ops[0]);
        };
        assert_eq!(
            p.collection.as_str(),
            "inbox",
            "the source row, not a target one"
        );
        assert_eq!(p.status, ReplicaStatus::Tombstone);
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
