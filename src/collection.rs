//! A mailbox, address book or calendar: a set of placements plus a sync
//! checkpoint.

use alloc::{string::String, vec::Vec};

/// The account-scoped identity of a collection.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Hash)]
pub struct CollectionId(pub String);

impl CollectionId {
    /// Borrows the id as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<&str> for CollectionId {
    fn from(value: &str) -> Self {
        Self(value.into())
    }
}

impl From<String> for CollectionId {
    fn from(value: String) -> Self {
        Self(value)
    }
}

/// An opaque per-collection sync token.
///
/// A QRESYNC pack, a JMAP state string, or a WebDAV sync-token; the engine
/// never inspects it, it only round-trips it between storage and remote.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Checkpoint(pub Vec<u8>);

/// A collection's metadata, independent of its placements.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Collection {
    /// The account-scoped id.
    pub id: CollectionId,
    /// The human-facing name.
    pub name: String,
    /// The last sync checkpoint, if ever synced.
    pub checkpoint: Option<Checkpoint>,
    /// Whether the probed spine is complete as of the checkpoint.
    pub enumerated: bool,
}
