//! Outbound remote changes and inbound storage writes.
//!
//! [`Change`] is what the engine asks the consumer to push to the remote;
//! [`WriteOp`] is what it asks the consumer to persist locally. The engine
//! itself performs neither: both travel as coroutine yields.

use alloc::vec::Vec;

use crate::{
    collection::{Checkpoint, CollectionId},
    object::Object,
    placement::{Base, Flags, Handle, Placement},
};

/// A change to push to the remote.
///
/// Membership is add or remove only; a move is a remove from the source
/// plus an add to the target. On backends without a native move this is a
/// copy then delete, acceptable for v1.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Change {
    /// Add a member, carrying the object body to upload.
    Add {
        /// The handle the member will be known by (provisional until the
        /// next enumerate reconciles it by link id).
        handle: Handle,
        /// The body to upload.
        object: Object,
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
