//! Outbound remote changes and inbound storage writes.
//!
//! [`ReplicaChange`] is what the engine asks the consumer to push to the
//! remote; [`ReplicaWriteOp`] is what it asks the consumer to persist
//! locally. The engine itself performs neither: both travel as coroutine
//! yields.

use alloc::{format, string::String, vec::Vec};

use crate::{
    collection::{ReplicaCheckpoint, ReplicaCollectionId},
    object::{ReplicaHash, ReplicaObject},
    placement::{ReplicaFlags, ReplicaHandle, ReplicaLinkId, ReplicaOrigin, ReplicaPlacement},
};

/// A change to push to the remote: what to do, and the key naming it.
///
/// Pushes are at-least-once: a crash between a serviced push and the
/// storage write that records it makes the next sync push the same change
/// again. The window is one chunk rather than one run (see
/// [`ReplicaSync::PUSH_CHUNK`](crate::sync::ReplicaSync::PUSH_CHUNK)), and
/// every change names itself through `key`, so a consumer that records the
/// keys it applied recognises a replay of any kind. Flag and content
/// pushes also re-apply harmlessly on their own; the other two are kept
/// harmless by treating a remove of an already-missing member as accepted,
/// and by using an add's `link_id` to detect that it already landed
/// instead of duplicating it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReplicaChange {
    /// What the remote is asked to do.
    pub kind: ReplicaChangeKind,
    /// The idempotency key naming this change, as
    /// [`new`](Self::new) derives it.
    pub key: ReplicaChangeKey,
}

impl ReplicaChange {
    /// Keys `kind` in `collection`.
    ///
    /// The only way a change is made, so it cannot exist without its key,
    /// and the key cannot disagree with what it names: the engine derives
    /// the kind, and keying it is the last thing that happens to it.
    pub fn new(collection: &ReplicaCollectionId, kind: ReplicaChangeKind) -> Self {
        let key = kind.key(collection);

        Self { kind, key }
    }

    /// The member this change acts on.
    pub fn handle(&self) -> &ReplicaHandle {
        match &self.kind {
            ReplicaChangeKind::Add { handle, .. } => handle,
            ReplicaChangeKind::Remove { handle, .. } => handle,
            ReplicaChangeKind::SetFlags { handle, .. } => handle,
            ReplicaChangeKind::Update { handle, .. } => handle,
        }
    }
}

/// What a [`ReplicaChange`] asks the remote to do.
///
/// Membership is add or remove only. An add reuses a server-side copy or
/// move when it carries an [`ReplicaOrigin`], else it uploads the stored
/// body (a genuine append).
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
pub enum ReplicaChangeKind {
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

impl ReplicaChangeKind {
    /// Derives this kind's idempotency key in `collection`.
    ///
    /// The key covers the collection, the handle, the kind and the target
    /// state the change makes true: the flag set of a
    /// [`SetFlags`](Self::SetFlags), the body of an [`Update`](Self::Update),
    /// the destination of a [`Remove`](Self::Remove), and the identity,
    /// markers, origin and body of an [`Add`](Self::Add). The same derived
    /// change keys the same on every run, and changes differing in any of
    /// those key differently.
    ///
    /// A precondition is deliberately not part of it: `if_match` states what
    /// the change was attempted against, not what it makes true, and a retry
    /// of one operation is one operation.
    fn key(&self, collection: &ReplicaCollectionId) -> ReplicaChangeKey {
        let handle = match self {
            Self::Add { handle, .. } => handle,
            Self::Remove { handle, .. } => handle,
            Self::SetFlags { handle, .. } => handle,
            Self::Update { handle, .. } => handle,
        };

        let mut digest = ReplicaChangeDigest::new();

        digest
            .field(collection.as_str().as_bytes())
            .field(handle.as_str().as_bytes());

        match self {
            Self::Add {
                link_id,
                flags,
                origin,
                object,
                ..
            } => {
                digest
                    .field(b"add")
                    .option(link_id.as_ref().map(|link| link.as_str().as_bytes()))
                    .flags(flags);
                match origin {
                    Some(origin) => digest
                        .field(b"1")
                        .field(origin.collection.as_str().as_bytes())
                        .field(origin.handle.as_str().as_bytes()),
                    None => digest.field(b"0"),
                }
                .option(object.as_ref().map(|hash| hash.as_str().as_bytes()));
            }
            Self::Remove { to, .. } => {
                digest
                    .field(b"remove")
                    .option(to.as_ref().map(|to| to.as_str().as_bytes()));
            }
            Self::SetFlags { flags, .. } => {
                digest.field(b"set-flags").flags(flags);
            }
            Self::Update { object, .. } => {
                digest.field(b"update").field(object.as_str().as_bytes());
            }
        }

        digest.finish()
    }
}

crate::replica_id! {
    /// The idempotency key naming a derived change, as
    /// [`ReplicaChange::new`] derives it.
    ///
    /// Sixteen lowercase hexadecimal characters; opaque to the engine, which
    /// never reads one back. A consumer records the keys it has applied and
    /// recognises a replay by looking one up.
    ReplicaChangeKey, Ord, PartialOrd, Hash,
}

/// The digest a [`ReplicaChangeKind`] is folded into to key it.
///
/// FNV-1a, sixty-four bits, computed here rather than pulled in as a
/// dependency: the crate has none beyond `log` and `thiserror`, and what an
/// idempotency key needs is determinism, not resistance to a forged
/// collision.
struct ReplicaChangeDigest(u64);

impl ReplicaChangeDigest {
    const OFFSET: u64 = 0xcbf29ce484222325;
    const PRIME: u64 = 0x100000001b3;

    fn new() -> Self {
        Self(Self::OFFSET)
    }

    /// Folds one field in, terminated so that two different splits of the
    /// same bytes key differently.
    fn field(&mut self, bytes: &[u8]) -> &mut Self {
        for byte in bytes.iter().chain(&[0]) {
            self.0 ^= u64::from(*byte);
            self.0 = self.0.wrapping_mul(Self::PRIME);
        }

        self
    }

    /// Folds an optional field in, a present and an absent one keying
    /// differently.
    fn option(&mut self, bytes: Option<&[u8]>) -> &mut Self {
        match bytes {
            Some(bytes) => self.field(b"1").field(bytes),
            None => self.field(b"0"),
        }
    }

    /// Folds a flag set in, counted first so a marker cannot pass for the
    /// field that follows the set. An unknown set is not an empty one.
    fn flags(&mut self, flags: &ReplicaFlags) -> &mut Self {
        let Some(flags) = flags.known() else {
            return self.field(b"unknown");
        };

        self.field(b"known").field(&flags.len().to_le_bytes());
        for flag in flags {
            self.field(flag.as_bytes());
        }

        self
    }

    fn finish(&self) -> ReplicaChangeKey {
        ReplicaChangeKey::from(format!("{:016x}", self.0))
    }
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
    /// references come from placement pointers only, so a stored object is
    /// unreferenced until an [`ReplicaWriteOp::UpsertPlacement`] points at its
    /// hash. That upsert usually rides in the same batch, but it need not: a
    /// consumer streaming bodies ahead of their metadata stores them in one
    /// batch and attaches them in a later one, and a storage backend must keep
    /// an unreferenced object rather than collect it at the commit.
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

#[cfg(test)]
mod tests {
    use alloc::{collections::BTreeSet, string::String, vec, vec::Vec};

    use crate::{
        change::{ReplicaChange, ReplicaChangeKey, ReplicaChangeKind},
        collection::ReplicaCollectionId,
        object::ReplicaHash,
        placement::{ReplicaFlags, ReplicaHandle, ReplicaLinkId, ReplicaOrigin},
    };

    fn key(collection: &ReplicaCollectionId, kind: ReplicaChangeKind) -> ReplicaChangeKey {
        ReplicaChange::new(collection, kind).key
    }

    fn set_flags(handle: &str, flags: &[&str]) -> ReplicaChangeKind {
        ReplicaChangeKind::SetFlags {
            handle: ReplicaHandle::from(handle),
            flags: ReplicaFlags::from_iter(flags.iter().copied()),
        }
    }

    fn update(handle: &str, object: &str, if_match: Option<&str>) -> ReplicaChangeKind {
        ReplicaChangeKind::Update {
            handle: ReplicaHandle::from(handle),
            object: ReplicaHash::from(object),
            if_match: if_match.map(String::from),
        }
    }

    #[test]
    fn the_same_derived_change_keys_the_same() {
        let inbox = ReplicaCollectionId::from("inbox");

        assert_eq!(
            key(&inbox, set_flags("1", &["seen"])),
            key(&inbox, set_flags("1", &["seen"])),
        );
    }

    #[test]
    fn a_key_separates_collection_handle_kind_and_target_state() {
        let inbox = ReplicaCollectionId::from("inbox");
        let archive = ReplicaCollectionId::from("archive");

        let keys: Vec<ReplicaChangeKey> = vec![
            // the collection
            key(&inbox, set_flags("1", &["seen"])),
            key(&archive, set_flags("1", &["seen"])),
            // the handle
            key(&inbox, set_flags("2", &["seen"])),
            // the target state
            key(&inbox, set_flags("1", &["flagged"])),
            key(&inbox, set_flags("1", &["seen", "flagged"])),
            key(&inbox, set_flags("1", &[])),
            // the kind, on one handle and one state
            key(&inbox, update("1", "aaa", None)),
            key(&inbox, update("1", "bbb", None)),
            key(
                &inbox,
                ReplicaChangeKind::Remove {
                    handle: ReplicaHandle::from("1"),
                    to: None,
                    link_id: None,
                    if_match: None,
                },
            ),
            key(
                &inbox,
                ReplicaChangeKind::Remove {
                    handle: ReplicaHandle::from("1"),
                    to: Some(archive.clone()),
                    link_id: None,
                    if_match: None,
                },
            ),
            key(
                &inbox,
                ReplicaChangeKind::Add {
                    handle: ReplicaHandle::from("1"),
                    link_id: None,
                    flags: ReplicaFlags::default(),
                    origin: None,
                    object: None,
                },
            ),
            key(
                &inbox,
                ReplicaChangeKind::Add {
                    handle: ReplicaHandle::from("1"),
                    link_id: Some(ReplicaLinkId::from("mid")),
                    flags: ReplicaFlags::default(),
                    origin: None,
                    object: None,
                },
            ),
            key(
                &inbox,
                ReplicaChangeKind::Add {
                    handle: ReplicaHandle::from("1"),
                    link_id: None,
                    flags: ReplicaFlags::default(),
                    origin: Some(ReplicaOrigin {
                        collection: archive.clone(),
                        handle: ReplicaHandle::from("9"),
                    }),
                    object: None,
                },
            ),
            key(
                &inbox,
                ReplicaChangeKind::Add {
                    handle: ReplicaHandle::from("1"),
                    link_id: None,
                    flags: ReplicaFlags::default(),
                    origin: None,
                    object: Some(ReplicaHash::from("aaa")),
                },
            ),
        ];

        let distinct: BTreeSet<&ReplicaChangeKey> = keys.iter().collect();
        assert_eq!(distinct.len(), keys.len(), "keys collided: {keys:?}");
    }

    #[test]
    fn a_precondition_is_not_part_of_the_key() {
        let inbox = ReplicaCollectionId::from("inbox");
        let keyed = key(&inbox, update("1", "aaa", None));

        assert_eq!(key(&inbox, update("1", "aaa", Some("r1"))), keyed);
        assert_eq!(key(&inbox, update("1", "aaa", Some("r2"))), keyed);
    }

    #[test]
    fn an_unknown_flag_set_does_not_key_as_an_empty_one() {
        let inbox = ReplicaCollectionId::from("inbox");
        let unknown = ReplicaChangeKind::SetFlags {
            handle: ReplicaHandle::from("1"),
            flags: ReplicaFlags::Unknown,
        };

        assert_ne!(key(&inbox, unknown), key(&inbox, set_flags("1", &[])));
    }
}
