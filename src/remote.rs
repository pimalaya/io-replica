//! Payloads exchanged with the remote seam.
//!
//! The consumer satisfies three capabilities by driving a protocol crate
//! (io-imap, io-jmap, io-webdav): enumerate, fetch and push. These types
//! are what they return.

use alloc::{string::String, vec::Vec};

use crate::{
    collection::ReplicaCheckpoint,
    object::ReplicaHash,
    placement::{ReplicaFlags, ReplicaHandle, ReplicaLinkId, ReplicaMeta, ReplicaSortKey},
};

/// The detail tier a fetch targets.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReplicaTier {
    /// Summary only: a header or property subset.
    Meta,
    /// The full item body; yields an object.
    Full,
}

/// One row of an enumerate snapshot: handle, flags and content revision,
/// no link id, no body.
///
/// Enough to run the three-way merge without fetching any body, which is
/// why a partial body cache stays safe. The link id is not here:
/// enumeration only has to yield handles, so it is resolved later at the
/// [`ReplicaTier::Meta`] fetch.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReplicaRemoteItem {
    /// The protocol handle.
    pub handle: ReplicaHandle,
    /// The current remote flag set.
    pub flags: ReplicaFlags,
    /// The current remote content revision, for mutable-content backends
    /// (a WebDAV etag, an MS Graph changeKey). `None` where content is
    /// immutable, which the merge reads as unchanged, never as unknown.
    pub revision: Option<String>,
}

/// The result of enumerating a collection: its full or delta member set
/// plus the new checkpoint.
///
/// `complete` tells the merge how to read the absence of a known local
/// handle. A complete snapshot lists every current member, so a local
/// placement missing from `items` was deleted upstream. A delta snapshot
/// (QRESYNC, a JMAP `/changes` query, a CalDAV sync-token) lists only
/// what changed since `cursor`: unlisted placements are untouched, and
/// removals arrive explicitly in `vanished`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReplicaRemoteSnapshot {
    /// The members observed: every current member when `complete`, else
    /// only those added or changed since the cursor.
    ///
    /// Sorted by handle and each handle listed once: the merge walks this
    /// beside the local placements rather than indexing it, which keeps a
    /// whole-collection sync from copying both sides to join them.
    /// Protocols hand it over sorted already. An unsorted snapshot is
    /// sorted by the engine and a handle listed twice collapsed to its
    /// first item, so getting it wrong costs a pass, not correctness.
    pub items: Vec<ReplicaRemoteItem>,
    /// Handles removed upstream since the cursor, always empty for a
    /// complete snapshot, where absence from `items` already means it.
    pub vanished: Vec<ReplicaHandle>,
    /// Whether `items` is the whole member set (true) or a delta (false).
    pub complete: bool,
    /// The checkpoint these items are current as of.
    pub checkpoint: ReplicaCheckpoint,
}

/// The body a `Full` fetch reports for an item.
///
/// A consumer that can stream MAY persist the body straight into its blob
/// store and report it as [`Persisted`](ReplicaFetchedBody::Persisted),
/// so the engine never holds a full body in memory;
/// [`Inline`](ReplicaFetchedBody::Inline) bytes are stored by the engine
/// instead. Either way the object is indexed from its `(hash, size)`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ReplicaFetchedBody {
    /// The body bytes and their content hash, for the engine to store.
    Inline {
        /// Content hash of the bytes.
        hash: ReplicaHash,
        /// The body bytes.
        bytes: Vec<u8>,
    },
    /// An object the consumer already persisted into its blob store, so
    /// the engine records it by `(hash, size)` and writes no bytes.
    Persisted {
        /// Content hash of the persisted object.
        hash: ReplicaHash,
        /// Size of the persisted object, in bytes.
        size: usize,
    },
}

/// The result of fetching one item at a requested tier.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReplicaFetchedItem {
    /// The fetched handle.
    pub handle: ReplicaHandle,
    /// The resolved link id.
    pub link_id: ReplicaLinkId,
    /// The cached summary, always set, projected from the body where the
    /// backend has no cheap summary tier.
    pub meta: ReplicaMeta,
    /// The presentation sort key, derived from the same parse as `meta`.
    /// Empty when the connector's kind defines none.
    pub sort_key: ReplicaSortKey,
    /// The body; `None` at [`ReplicaTier::Meta`].
    pub body: Option<ReplicaFetchedBody>,
    /// The remote content revision the fetched body corresponds to, for
    /// mutable-content backends; `None` where content is immutable.
    pub revision: Option<String>,
}

/// The outcome of pushing one change.
///
/// Pushes are at-least-once (see [`crate::change::ReplicaChange`]): a
/// remove whose target is already missing means the delete landed, so the
/// consumer reports it [`ReplicaPushOutcome::Accepted`]; a rejection
/// keeps the tombstone retrying forever.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReplicaPushOutcome {
    /// The remote accepted the change.
    Accepted,
    /// Optimistic concurrency rejected it: the base was stale.
    Rejected,
}

/// The result of pushing one change.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReplicaPushResult {
    /// The handle the change targeted (the provisional one for an add).
    pub handle: ReplicaHandle,
    /// Whether the remote accepted it.
    pub outcome: ReplicaPushOutcome,
    /// For an accepted add, the server-assigned handle the engine rekeys
    /// the provisional placement to; `None` for flag and remove pushes.
    pub assigned: Option<ReplicaHandle>,
    /// For an accepted push that wrote content, the content revision the
    /// remote now holds; `None` when the remote reports none, and for
    /// flag and remove pushes.
    pub revision: Option<String>,
}
