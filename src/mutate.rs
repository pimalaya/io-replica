//! I/O-free coroutine to mutate a collection locally, with no network.
//!
//! Loads the target placement, applies the change in memory, marks it
//! dirty or tombstone (the base is left untouched so the next sync can
//! derive the pending push), and writes it back. The remote is never
//! touched here; reconciliation is [`crate::sync`]'s job.

use core::fmt;

use alloc::string::String;

use log::{debug, trace};
use thiserror::Error;

use crate::{
    change::WriteOp,
    collection::CollectionId,
    coroutine::*,
    placement::{Flags, Handle, Origin, Placement, Status},
};

/// A local edit applied offline.
///
/// Each mutation reads one source placement in the coroutine's collection
/// and stages the resulting writes; the remote is reconciled on the next
/// sync. A copy stages a [`Status::Created`] placement in another collection.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Mutation {
    /// Replace a placement's flag set.
    SetFlags {
        /// The placement to update.
        handle: Handle,
        /// The new flag set.
        flags: Flags,
    },
    /// Mark a placement deleted, keeping it as a tombstone until synced.
    Remove(Handle),
    /// Copy a placement into `target` as a pending create that the next
    /// sync pushes with a server-side copy (no body re-upload). The source
    /// is left untouched.
    Copy {
        /// The source placement to copy.
        handle: Handle,
        /// The collection to copy it into.
        target: CollectionId,
        /// The provisional handle the copy is staged under in `target`.
        placeholder: Handle,
    },
}

impl Mutation {
    /// The source handle the mutation reads in the coroutine's collection.
    fn handle(&self) -> &Handle {
        match self {
            Self::SetFlags { handle, .. } => handle,
            Self::Remove(handle) => handle,
            Self::Copy { handle, .. } => handle,
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
    collection: CollectionId,
    mutation: Mutation,
    state: State,
}

impl OfflineMutate {
    /// Creates a coroutine that applies `mutation` to `collection`.
    pub fn new(collection: impl Into<CollectionId>, mutation: Mutation) -> Self {
        let collection = collection.into();
        debug!("mutate collection {}", collection.as_str());

        Self {
            collection,
            mutation,
            state: State::Start,
        }
    }

    /// Stages the writes for the mutation given its loaded source placement.
    /// Flags and removes rewrite the source in place; a copy leaves the
    /// source untouched and stages a pending create in the target.
    fn writes(&self, mut source: Placement) -> alloc::vec::Vec<WriteOp> {
        match &self.mutation {
            Mutation::SetFlags { flags, .. } => {
                source.flags = flags.clone();
                source.status = Status::Dirty;
                alloc::vec![WriteOp::UpsertPlacement(source)]
            }
            Mutation::Remove(_) => {
                source.status = Status::Tombstone;
                alloc::vec![WriteOp::UpsertPlacement(source)]
            }
            Mutation::Copy {
                target,
                placeholder,
                ..
            } => {
                let create = Placement {
                    collection: target.clone(),
                    handle: placeholder.clone(),
                    link_id: source.link_id.clone(),
                    object: source.object.clone(),
                    level: source.level,
                    meta: source.meta.clone(),
                    flags: source.flags.clone(),
                    status: Status::Created,
                    base: None,
                    origin: Some(Origin {
                        collection: source.collection.clone(),
                        handle: source.handle.clone(),
                    }),
                };
                alloc::vec![WriteOp::UpsertPlacement(create)]
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
    use crate::{
        mutate::*,
        placement::{Base, Level, Status},
        storage::Loaded,
    };

    fn loaded(handle: &str) -> Loaded {
        Loaded {
            placements: vec![Placement {
                collection: "inbox".into(),
                handle: Handle::from(handle),
                link_id: None,
                object: None,
                level: Level::Meta,
                meta: None,
                flags: Flags::default(),
                status: Status::Clean,
                base: Some(Base {
                    flags: Flags::default(),
                    present: true,
                    etag: None,
                }),
                origin: None,
            }],
            checkpoint: None,
        }
    }

    #[test]
    fn set_flags_marks_dirty() {
        let mutation = Mutation::SetFlags {
            handle: Handle::from("1"),
            flags: Flags::from_iter(["seen"]),
        };
        let mut mutate = OfflineMutate::new("inbox", mutation);
        let _ = mutate.resume(None);

        let ops = match mutate.resume(Some(OfflineArg::Load(loaded("1")))) {
            OfflineCoroutineState::Yielded(OfflineYield::WantsWrite(ops)) => ops,
            state => panic!("expected WantsWrite, got {state:?}"),
        };
        let WriteOp::UpsertPlacement(p) = &ops[0] else {
            panic!("expected UpsertPlacement, got {:?}", ops[0]);
        };
        assert_eq!(p.status, Status::Dirty);
        assert!(p.flags.contains("seen"));
        assert!(p.base.is_some(), "base must be preserved for sync");
    }

    #[test]
    fn remove_marks_tombstone() {
        let mutation = Mutation::Remove(Handle::from("1"));
        let mut mutate = OfflineMutate::new("inbox", mutation);
        let _ = mutate.resume(None);

        let ops = match mutate.resume(Some(OfflineArg::Load(loaded("1")))) {
            OfflineCoroutineState::Yielded(OfflineYield::WantsWrite(ops)) => ops,
            state => panic!("expected WantsWrite, got {state:?}"),
        };
        let WriteOp::UpsertPlacement(p) = &ops[0] else {
            panic!("expected UpsertPlacement, got {:?}", ops[0]);
        };
        assert_eq!(p.status, Status::Tombstone);
    }

    #[test]
    fn unknown_handle_errors() {
        let mutation = Mutation::Remove(Handle::from("nope"));
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
        let mutation = Mutation::Remove(Handle::from("1"));
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
        let mutation = Mutation::Remove(Handle::from("1"));
        let mut mutate = OfflineMutate::new("inbox", mutation);
        let _ = mutate.resume(None);

        match mutate.resume(None) {
            OfflineCoroutineState::Complete(Err(OfflineMutateError::MissingArg)) => {}
            state => panic!("expected MissingArg, got {state:?}"),
        }
    }

    #[test]
    fn copy_stages_created_placement_in_target() {
        // A copy leaves the source untouched and stages a Created placement
        // in the target under the placeholder, carrying its origin.
        let mutation = Mutation::Copy {
            handle: Handle::from("1"),
            target: "archive".into(),
            placeholder: Handle::from("tmp-1"),
        };
        let mut mutate = OfflineMutate::new("inbox", mutation);
        let _ = mutate.resume(None);

        let ops = match mutate.resume(Some(OfflineArg::Load(loaded("1")))) {
            OfflineCoroutineState::Yielded(OfflineYield::WantsWrite(ops)) => ops,
            state => panic!("expected WantsWrite, got {state:?}"),
        };
        let WriteOp::UpsertPlacement(p) = &ops[0] else {
            panic!("expected UpsertPlacement, got {:?}", ops[0]);
        };
        assert_eq!(p.collection.as_str(), "archive");
        assert_eq!(p.handle.as_str(), "tmp-1");
        assert_eq!(p.status, Status::Created);
        assert!(p.base.is_none());
        let origin = p.origin.as_ref().expect("the copy carries its origin");
        assert_eq!(origin.collection.as_str(), "inbox");
        assert_eq!(origin.handle.as_str(), "1");
    }
}
