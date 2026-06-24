//! An item's presence in one collection: handle, link id, level, flags
//! and sync base.
//!
//! A placement is one of the two identity axes (see [`crate::object`] for
//! the other). It pins a logical item to a single collection through the
//! protocol [`Handle`], carries the per-location mutable state (flags,
//! membership), records the detail [`Level`], and holds the [`Base`] the
//! three-way merge reconciles against.

use alloc::{
    collections::BTreeSet,
    string::{String, ToString},
};

use crate::{collection::CollectionId, object::Hash};

/// The protocol's per-collection location of an item.
///
/// IMAP uidvalidity plus uid, WebDAV href, JMAP id; always a string so
/// non-integer ids are a non-issue. Identifies a placement within its
/// collection.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Hash)]
pub struct Handle(pub String);

impl Handle {
    /// Borrows the handle as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<&str> for Handle {
    fn from(value: &str) -> Self {
        Self(value.into())
    }
}

impl From<String> for Handle {
    fn from(value: String) -> Self {
        Self(value)
    }
}

/// The logical-item identity used to group copies and map across
/// protocols.
///
/// A source global id (a provider message id, a JMAP id), else a stable
/// content id (the Message-ID header, the vCard or iCalendar UID). Never
/// derived from a size or other per-copy value a provider may rewrite.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Hash)]
pub struct LinkId(pub String);

impl LinkId {
    /// Borrows the link id as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<&str> for LinkId {
    fn from(value: &str) -> Self {
        Self(value.into())
    }
}

impl From<String> for LinkId {
    fn from(value: String) -> Self {
        Self(value)
    }
}

/// A minimal cached summary: enough for a list row and to resolve the
/// link id without a body.
///
/// Opaque to the engine (json in the reference store: sender, title, date
/// and the like; projected from the body where the backend has no cheap
/// summary tier).
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Meta(pub String);

/// An item's set of state markers, normalized by the consumer.
///
/// A plain string set so the engine stays protocol-agnostic; the consumer
/// folds equivalent spellings (for example `\Seen`, `$seen`, `seen`)
/// before they reach here. Backends without per-item markers leave it
/// empty.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Flags(pub BTreeSet<String>);

impl Flags {
    /// Reports whether `flag` is present.
    pub fn contains(&self, flag: &str) -> bool {
        self.0.contains(flag)
    }
}

impl<S: ToString> FromIterator<S> for Flags {
    fn from_iter<I: IntoIterator<Item = S>>(flags: I) -> Self {
        Self(flags.into_iter().map(|f| f.to_string()).collect())
    }
}

/// The detail level of a placement, a strict ladder where each rung
/// includes the one below.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum Level {
    /// Handle known, nothing else; kept complete per collection.
    Probed,
    /// Minimal summary cached.
    Meta,
    /// Linked to a stored object body.
    Full,
}

/// How a placement relates to its sync base.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Status {
    /// In sync with the base; no pending push.
    Clean,
    /// Locally changed since the base; a push is pending.
    Dirty,
    /// Locally deleted since the base; a remove is pending.
    Tombstone,
    /// Both sides changed and diverged; awaiting keep-both resolution.
    Conflict,
}

/// The last-synced state a placement reconciles against.
///
/// Where content is immutable only flags and membership mutate, so the
/// base is `{flags, present}`; where content is mutable, `etag` holds the
/// last-synced content identity so an in-place edit is detected.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Base {
    /// Last-synced flag set.
    pub flags: Flags,
    /// Last-synced membership in the collection.
    pub present: bool,
    /// Last-synced content identity for mutable-content backends.
    pub etag: Option<String>,
}

/// One item's presence in one collection.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Placement {
    /// The collection this placement belongs to.
    pub collection: CollectionId,
    /// The protocol handle within that collection.
    pub handle: Handle,
    /// The cross-collection link id; `None` until [`Level::Meta`].
    pub link_id: Option<LinkId>,
    /// The stored object body; `None` until [`Level::Full`].
    pub object: Option<Hash>,
    /// The current detail level.
    pub level: Level,
    /// The cached summary; `None` until [`Level::Meta`].
    pub meta: Option<Meta>,
    /// The current flag set.
    pub flags: Flags,
    /// How this placement relates to its base.
    pub status: Status,
    /// The last-synced base; `None` until first reconciled.
    pub base: Option<Base>,
}
