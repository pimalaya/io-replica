#![no_std]
#![cfg_attr(docsrs, feature(doc_cfg))]

//! # io-replica
//!
//! I/O-free offline-first replica engine for collections of PIM items:
//! mailboxes of messages, address books of contacts, calendars of
//! events. It maintains a local replica of remote collections, usable
//! fully offline, and reconciles that replica with the remote through a
//! three-way merge against a stored base. Sync is a consequence of
//! offline editing, not the primary goal.
//!
//! The engine owns no wire protocol and no on-disk format: it speaks
//! two seams, [`remote`] and [`storage`], both expressed as coroutine
//! yields, so a consumer wires it to whatever it likes (sqlite plus a
//! blob dir on desktop, io-imap over JNI plus an Android sqlite store
//! on mobile, io-webdav under an address book, etc). It ships two of
//! the three standard layers: the I/O-free coroutine core and the
//! [`client`] reference driver, itself free of std since it performs no
//! I/O of its own; there is no CLI, and no cargo feature, since no
//! layer pulls extra crates (the golden rule for feature-gating). The
//! finer operational details (the push-outcome discipline, the create
//! path, the checkpoint depths, driving the engine from an app) are
//! documented in the module headers, and a complete runnable lifecycle
//! lives in [examples/mailbox_lifecycle.rs][example].
//!
//! [example]: https://github.com/pimalaya/io-replica/blob/master/examples/mailbox_lifecycle.rs
//!
//! ## Two identity axes, never collapsed
//!
//! The engine separates what an item is from where it sits, and never
//! folds the two together; this split is what makes dedup, the unified
//! across-collections view and a safe partial cache fall out for free.
//! An [`object::ReplicaObject`] is the content-addressed body, stored
//! once and refcounted so copy, move and undelete are reference edits.
//! A [`placement::ReplicaPlacement`] is one item's presence in one
//! collection, pinned through a protocol [`placement::ReplicaHandle`]
//! (an IMAP UID, a WebDAV href, a JMAP id, always a string). Many
//! placements may point at one object, keyed across collections by a
//! stable [`placement::ReplicaLinkId`] (a Message-ID header, a vCard or
//! iCalendar UID), so a copied or shared item is fetched and stored
//! once.
//!
//! Each placement sits at one rung of a strict level ladder
//! ([`placement::ReplicaLevel`]), each rung including the one below:
//! probed (the handle is known and the spine is kept complete per
//! collection, so a missing item means deleted only when the base says
//! so, never inferred from a missing body), meta (a minimal summary
//! cached), and full (linked to a stored object body). The completeness
//! of the probed spine plus the per-placement
//! [`placement::ReplicaBase`], not the presence of a cached body, is
//! what tells deleted from not-cached, and keeps a partial body cache
//! safe to sync.
//!
//! ## The five verbs
//!
//! Every verb is an I/O-free state machine implementing the contract in
//! [`coroutine`]. [`open`] loads a collection with no network,
//! [`upgrade`] pulls a higher level (deduping against the object store
//! before fetching a body), [`mutate`] writes flags, content or
//! membership locally and marks the placement dirty, [`sync`]
//! reconciles a collection against its remote, and [`rekey`] rebuilds a
//! collection after a handle-space change (an IMAP UIDVALIDITY bump)
//! carrying the cache and pending state over by link id.
//!
//! Every verb is scoped to one collection, because the collection is
//! the unit of consistency: the spine completeness, the checkpoint and
//! the merge all live per collection. There is deliberately no
//! cross-collection read verb; the engine only reads local state in
//! order to merge it, and the merged, deduplicated across-collections
//! view is a plain query the consumer runs against its own storage
//! (placements sharing a link id point at one object), with no engine
//! involvement. [`open`] exists for symmetry as the reference
//! single-collection read, not as the only way to look at the data: the
//! storage is the consumer's, and the consumer may read it however it
//! likes.
//!
//! ## The three-way merge
//!
//! [`sync`] loads local state, enumerates the remote (full or
//! incremental), then merges local, base and remote per placement,
//! keyed on the handle, comparing per-placement identities (the flag
//! set, and for mutable-content backends a content revision) rather
//! than raw bytes. Flags merge element-wise and never conflict: the
//! side that changed a flag wins for that flag, and divergent sets fold
//! into their union. An edit beats a delete in both directions: a
//! remote update resurrects a local tombstone, and a local staged edit
//! survives a remote delete as a pending create. Diverging content is
//! marked conflicted, carrying the observed remote revision, for the
//! consumer to merge and resolve with an edit.
//!
//! A push is always confirmed before local state is rewritten: the
//! merge stashes the derived change and waits for the remote outcome,
//! rebasing on accept and retrying on reject. Holding a delete until
//! the server confirms is what keeps an incremental (QRESYNC) replica
//! from permanently desyncing.
//!
//! ## Seams and layout
//!
//! The two seams are [`remote`] and [`storage`]; their payloads and the
//! outbound [`change`] types (what to push, what to persist) are the
//! seam vocabulary. The item model lives in [`object`], [`placement`]
//! and [`collection`]. The five verbs are [`open`], [`upgrade`],
//! [`mutate`], [`sync`] and [`rekey`], all built on the [`coroutine`]
//! contract. The [`client`] module is the reference blocking driver,
//! servicing every yield through a consumer-implemented
//! [`client::ReplicaStorage`] and [`client::ReplicaRemote`]. The
//! [`hub`] module composes the single-remote merge into multi-source
//! sync (mirror, two-way) without a cross-merge: a storage wraps it to
//! project a per-source view of one shared item and absorb the writes
//! back.
//!
//! ## Conventions
//!
//! The crate is no_std with alloc, everywhere: even the blocking driver
//! never touches std. Public items carry the `Replica` prefix. Logging
//! follows the library rules: state changes at debug level, in-process
//! steps and data at trace level.

extern crate alloc;

/// Declares one of the crate's string-newtype identities.
///
/// Five of them exist (a handle, a link id, a collection id, a source id, a
/// content hash), and each was the same fifteen lines: the tuple struct, an
/// `as_str`, and a conversion from each of the two string types. They differ
/// only in what they mean and in which traits they derive, so that is all the
/// macro takes; the doc comments stay on the call, which is where a reader
/// looks for them.
macro_rules! replica_id {
    (
        $(#[$meta:meta])*
        $name:ident $(, $derive:ident)* $(,)?
    ) => {
        $(#[$meta])*
        #[derive(Clone, Debug, Eq, PartialEq $(, $derive)*)]
        pub struct $name(pub alloc::string::String);

        impl $name {
            #[doc = concat!("Borrows the ", stringify!($name), " as a string slice.")]
            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl From<&str> for $name {
            fn from(value: &str) -> Self {
                Self(value.into())
            }
        }

        impl From<alloc::string::String> for $name {
            fn from(value: alloc::string::String) -> Self {
                Self(value)
            }
        }
    };
}

pub(crate) use replica_id;

pub mod change;
pub mod client;
pub mod collection;
pub mod coroutine;
pub mod hub;
pub mod mutate;
pub mod object;
pub mod open;
pub mod placement;
pub mod rekey;
pub mod remote;
pub mod storage;
pub mod sync;
#[cfg(test)]
mod testlog;
pub mod upgrade;
