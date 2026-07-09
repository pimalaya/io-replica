//! Outbound remote changes and inbound storage writes.
//!
//! [`Change`] is what the engine asks the consumer to push to the remote;
//! [`WriteOp`] is what it asks the consumer to persist locally. The engine
//! itself performs neither: both travel as coroutine yields.

use alloc::{string::String, vec::Vec};

use crate::{
    collection::{Checkpoint, CollectionId},
    object::{Hash, Object},
    placement::{Flags, Handle, LinkId, Origin, Placement},
};

/// A change to push to the remote.
///
/// Membership is add or remove only; a move is the target add plus the
/// source remove. An add reuses a server-side copy or move when it carries
/// an [`Origin`], else it uploads the stored body (a genuine append).
///
/// Pushes are at-least-once: a crash between a serviced push and the
/// storage write that records it makes the next sync push the same change
/// again. Flag and content pushes re-apply harmlessly; the consumer keeps
/// the retries of the other two harmless by treating a remove of an
/// already-missing member as accepted, and by using an add's `link_id` to
/// detect that it already landed instead of duplicating it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Change {
    /// Add a member. The push reconciles the provisional `handle` to the
    /// server-assigned one (returned as [`crate::remote::PushResult::assigned`]).
    Add {
        /// The provisional handle the member is staged under locally.
        handle: Handle,
        /// The logical-item identity, when already resolved; the
        /// idempotency key for a retried add.
        link_id: Option<LinkId>,
        /// The flag set to create the member with (an IMAP APPEND flag
        /// list). A server-side copy may ignore it (the copy inherits the
        /// source flags; any skew reconciles on the next sync).
        flags: Flags,
        /// Where the body already lives, for a server-side copy or move;
        /// `None` for an append that uploads `object`. When the server
        /// refuses the copy because the origin is gone (expunged), a
        /// consumer holding `object` may fall back to uploading it;
        /// without a body, rejecting keeps the pending create visible.
        origin: Option<Origin>,
        /// The stored body to upload when there is no `origin` (an
        /// append); the consumer resolves the bytes from its object store.
        object: Option<Hash>,
    },
    /// Remove a member. `to` is the collection to move it into (an offline
    /// move, a server-side UID MOVE); `None` is a plain delete, which the
    /// consumer routes to trash.
    Remove {
        /// The member to remove.
        handle: Handle,
        /// The move destination, or `None` for a delete.
        to: Option<CollectionId>,
        /// The last-synced content revision, as an optimistic-concurrency
        /// precondition (a WebDAV If-Match); `None` where content is
        /// immutable or never synced with one.
        if_match: Option<String>,
    },
    /// Replace a member's flag set.
    SetFlags {
        /// The member to update.
        handle: Handle,
        /// The new flag set.
        flags: Flags,
    },
    /// Replace a member's content in place with a locally edited body.
    Update {
        /// The member to update.
        handle: Handle,
        /// The hash of the new body in the object store.
        object: Hash,
        /// The last-synced content revision, as an optimistic-concurrency
        /// precondition (a WebDAV If-Match); `None` when never based on
        /// one.
        if_match: Option<String>,
    },
}

/// A write to persist in local storage.
///
/// The set the four verbs emit; the consumer applies them atomically
/// against its index and blob store.
///
/// Object references derive from placement pointers: a stored placement
/// references an object once per pointing field (`Placement::object` and
/// `Base::object`). The consumer maintains the counts incrementally, by
/// diffing an upsert against the stored row it replaces and by releasing
/// both pointers of a dropped row; an object no other placement points at
/// may be garbage-collected.
// NOTE: upserts dominate every write batch, so boxing the placement to
// shrink the enum would only add indirection on the hot variant.
#[allow(clippy::large_enum_variant)]
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
    /// Store an object body. Storing takes no reference of its own:
    /// references come from placement pointers only, and a paired
    /// [`WriteOp::UpsertPlacement`] pointing at the hash lands in the same
    /// batch.
    StoreObject {
        /// The object metadata.
        object: Object,
        /// The body bytes.
        body: Vec<u8>,
    },
    /// Set a collection's sync checkpoint.
    SetCheckpoint {
        /// The collection to checkpoint.
        collection: CollectionId,
        /// The new checkpoint.
        checkpoint: Checkpoint,
    },
}
