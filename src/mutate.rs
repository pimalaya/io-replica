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
    placement::{Flags, Handle, Placement, Status},
};

/// A local edit applied offline.
///
/// Membership add and content creation belong to the create and sync
/// paths, not to a local mutation; v1 mutates flags and removes only.
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
}

impl Mutation {
    fn handle(&self) -> &Handle {
        match self {
            Self::SetFlags { handle, .. } => handle,
            Self::Remove(handle) => handle,
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

    fn apply(&self, mut placement: Placement) -> Placement {
        match &self.mutation {
            Mutation::SetFlags { flags, .. } => {
                placement.flags = flags.clone();
                placement.status = Status::Dirty;
            }
            Mutation::Remove(_) => {
                placement.status = Status::Tombstone;
            }
        }

        placement
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

                let updated = self.apply(placement);
                debug!(
                    "stage local change on {}, status {:?}",
                    handle.as_str(),
                    updated.status
                );
                trace!("updated placement: {updated:?}");

                self.state = State::PendingWrite;
                let ops = vec![WriteOp::UpsertPlacement(updated)];
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
}
