//! Outbound remote changes and inbound storage writes.
//!
//! [`Change`] is what the engine asks the consumer to push to the remote;
//! [`WriteOp`] is what it asks the consumer to persist locally. The engine
//! itself performs neither: both travel as coroutine yields.

use alloc::vec::Vec;

use crate::{
    collection::{Checkpoint, CollectionId},
    object::Object,
    placement::{Base, Flags, Handle, Origin, Placement},
};

/// A change to push to the remote.
///
/// Membership is add or remove only; a move is the target add plus the
/// source remove. An add reuses a server-side copy or move when it carries
/// an [`Origin`], else it uploads the [`Object`] body (a genuine append).
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Change {
    /// Add a member. The push reconciles the provisional `handle` to the
    /// server-assigned one (returned as [`crate::remote::PushResult::assigned`]).
    Add {
        /// The provisional handle the member is staged under locally.
        handle: Handle,
        /// Where the body already lives, for a server-side copy or move;
        /// `None` for an append that uploads `object`.
        origin: Option<Origin>,
        /// The body to upload when there is no `origin` (an append).
        object: Option<Object>,
    },
    /// Remove a member.
    Remove(Handle),
    /// Replace a member's flag set.
    SetFlags {
        /// The member to update.
        handle: Handle,
        /// The new flag set.
        flags: Flags,
    },
}

/// A write to persist in local storage.
///
/// The set the four verbs emit; the consumer applies them atomically
/// against its index and blob store.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum WriteOp {
    /// Insert or replace a placement.
    UpsertPlacement(Placement),
    /// Drop a placement (delete or remote-side disappearance).
    DropPlacement {
        /// The owning collection.
        collection: CollectionId,
        /// The handle to drop.
        handle: Handle,
    },
    /// Store an object body, bumping its refcount.
    StoreObject {
        /// The object metadata.
        object: Object,
        /// The body bytes.
        body: Vec<u8>,
    },
    /// Set a placement's base after reconciling.
    SetBase {
        /// The owning collection.
        collection: CollectionId,
        /// The handle to rebase.
        handle: Handle,
        /// The new base.
        base: Base,
    },
    /// Set a collection's sync checkpoint.
    SetCheckpoint {
        /// The collection to checkpoint.
        collection: CollectionId,
        /// The new checkpoint.
        checkpoint: Checkpoint,
    },
}
