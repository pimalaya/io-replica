//! Outbound remote changes and inbound storage writes.
//!
//! [`ReplicaChange`] is what the engine asks the consumer to push to the
//! remote; [`ReplicaWriteOp`] is what it asks the consumer to persist
//! locally. The engine itself performs neither: both travel as coroutine
//! yields.

use alloc::{string::String, vec::Vec};

use crate::{
    collection::{ReplicaCheckpoint, ReplicaCollectionId},
    object::{ReplicaHash, ReplicaObject},
    placement::{ReplicaFlags, ReplicaHandle, ReplicaLinkId, ReplicaOrigin, ReplicaPlacement},
};

/// A change to push to the remote.
///
/// Membership is add or remove only. An add reuses a server-side copy or
/// move when it carries an [`ReplicaOrigin`], else it uploads the stored
/// body (a genuine append).
///
/// Pushes are at-least-once: a crash between a serviced push and the
/// storage write that records it makes the next sync push the same change
/// again. Flag and content pushes re-apply harmlessly; the consumer keeps
/// the retries of the other two harmless by treating a remove of an
/// already-missing member as accepted, and by using an add's `link_id` to
/// detect that it already landed instead of duplicating it.
///
/// # A move is two halves that must not both deliver
///
/// A move is staged as a create in the target plus a remove of the source
/// (see [`ReplicaMutation::Move`](crate::mutate::ReplicaMutation::Move)),
/// each derived by its own collection's sync, in whichever order the
/// consumer runs them. Both halves can deliver the item on their own: the
/// create by copying from its origin, the remove by relocating the member
/// into `to`. So a [`Remove`](Self::Remove) carries the `link_id` its `to`
/// would receive, and a consumer SHALL relocate only while the
/// destination does not already hold it; otherwise the create already
/// delivered, and the remove is a plain delete of the source.
///
/// Neither half may be dropped in favour of the other. The remove is what
/// keeps a move safe when the target syncs last (the source is relocated
/// rather than deleted out from under a copy that never ran), and the
/// create is what keeps it working through a [hub](crate::hub), whose
/// bindings carry no origin. When the target syncs last its create finds
/// its origin already relocated: the push is rejected and the placeholder
/// stays visibly pending, since an add carries no key that separates a
/// second copy the user asked for from one the remove already served.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ReplicaChange {
    /// Add a member. The push reconciles the provisional `handle` to the
    /// server-assigned one (returned as
    /// [`crate::remote::ReplicaPushResult::assigned`]).
    Add {
        /// The provisional handle the member is staged under locally.
        handle: ReplicaHandle,
        /// The logical-item identity, when already resolved; the
        /// idempotency key for a retried add.
        link_id: Option<ReplicaLinkId>,
        /// The flag set to create the member with (an IMAP APPEND flag
        /// list). A server-side copy may ignore it (the copy inherits the
        /// source flags; any skew reconciles on the next sync).
        flags: ReplicaFlags,
        /// Where the body already lives, for a server-side copy or move;
        /// `None` for an append that uploads `object`. When the server
        /// refuses the copy because the origin is gone (expunged), a
        /// consumer holding `object` may fall back to uploading it;
        /// without a body, rejecting keeps the pending create visible.
        origin: Option<ReplicaOrigin>,
        /// The stored body to upload when there is no `origin` (an
        /// append); the consumer resolves the bytes from its object store.
        object: Option<ReplicaHash>,
    },
    /// Remove a member. `to` is the collection to move it into (an offline
    /// move, a server-side UID MOVE); `None` is a plain delete, whose
    /// disposal (expunge, trash) is the consumer's policy.
    Remove {
        /// The member to remove.
        handle: ReplicaHandle,
        /// The move destination, or `None` for a delete.
        to: Option<ReplicaCollectionId>,
        /// The logical-item identity, when already resolved: the delivery
        /// key of a move. A `to` that already holds this link id was
        /// served by the move's other half, so the remove is a plain
        /// delete rather than a second relocation.
        link_id: Option<ReplicaLinkId>,
        /// The last-synced content revision, as an optimistic-concurrency
        /// precondition (a WebDAV If-Match); `None` where content is
        /// immutable or never synced with one.
        if_match: Option<String>,
    },
    /// Replace a member's flag set.
    SetFlags {
        /// The member to update.
        handle: ReplicaHandle,
        /// The new flag set.
        flags: ReplicaFlags,
    },
    /// Replace a member's content in place with a locally edited body.
    Update {
        /// The member to update.
        handle: ReplicaHandle,
        /// The hash of the new body in the object store.
        object: ReplicaHash,
        /// The last-synced content revision, as an optimistic-concurrency
        /// precondition (a WebDAV If-Match); `None` when never based on
        /// one.
        if_match: Option<String>,
    },
}

/// Why a placement is being dropped.
///
/// The engine drops a row for two unrelated reasons, and a storage that
/// shares one item across sources (a [hub](crate::hub)) has to tell them
/// apart: propagating a delete the engine never meant is how a
/// housekeeping drop becomes data loss on another source.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReplicaDropReason {
    /// The item is gone: a local delete the remote confirmed, or a member
    /// that vanished upstream. A shared item is deleted everywhere.
    Deleted,
    /// Only this row is gone, superseded by another the same batch writes:
    /// a provisional placeholder reconciled to its server-assigned handle,
    /// a spine rebuilt onto a new handle space. The item lives on.
    Superseded,
}

/// A write to persist in local storage.
///
/// The set the four verbs emit; the consumer applies them atomically
/// against its index and blob store.
///
/// Object references derive from placement pointers: a stored placement
/// references an object once per pointing field (`ReplicaPlacement::object` and
/// `ReplicaBase::object`). The consumer maintains the counts incrementally, by
/// diffing an upsert against the stored row it replaces and by releasing
/// both pointers of a dropped row; an object no other placement points at
/// may be garbage-collected.
// NOTE: upserts dominate every write batch, so boxing the placement to
// shrink the enum would only add indirection on the hot variant.
#[allow(clippy::large_enum_variant)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ReplicaWriteOp {
    /// Insert or replace a placement.
    UpsertPlacement(ReplicaPlacement),
    /// Drop a placement.
    DropPlacement {
        /// The owning collection.
        collection: ReplicaCollectionId,
        /// The handle to drop.
        handle: ReplicaHandle,
        /// Whether the item itself is gone, or only this row of it.
        reason: ReplicaDropReason,
    },
    /// Store an object body. Storing takes no reference of its own:
    /// references come from placement pointers only, and a paired
    /// [`ReplicaWriteOp::UpsertPlacement`] pointing at the hash lands in the
    /// same batch.
    StoreObject {
        /// The object metadata.
        object: ReplicaObject,
        /// The body bytes, or `None` when the consumer already persisted the
        /// object into its blob store during a streaming fetch — the engine
        /// then only records the object (`object`), writing no bytes.
        body: Option<Vec<u8>>,
    },
    /// Set a collection's sync checkpoint.
    SetCheckpoint {
        /// The collection to checkpoint.
        collection: ReplicaCollectionId,
        /// The new checkpoint.
        checkpoint: ReplicaCheckpoint,
    },
}
