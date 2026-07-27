//! A mailbox, address book or calendar: a set of placements plus a sync
//! checkpoint.

use alloc::{string::String, vec::Vec};

/// The account-scoped identity of a collection.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Hash)]
pub struct ReplicaCollectionId(pub String);

impl ReplicaCollectionId {
    /// Borrows the id as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<&str> for ReplicaCollectionId {
    fn from(value: &str) -> Self {
        Self(value.into())
    }
}

impl From<String> for ReplicaCollectionId {
    fn from(value: String) -> Self {
        Self(value)
    }
}

/// An opaque per-collection sync token.
///
/// A QRESYNC pack, a JMAP state string, or a WebDAV sync-token; the engine
/// never inspects it, it only round-trips it between storage and remote.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ReplicaCheckpoint(pub Vec<u8>);

/// A collection's metadata, independent of its placements.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReplicaCollection {
    /// The account-scoped id.
    pub id: ReplicaCollectionId,
    /// The human-facing name.
    pub name: String,
    /// The last sync checkpoint, if ever synced.
    pub checkpoint: Option<ReplicaCheckpoint>,
    /// Whether the probed spine is complete as of the checkpoint.
    pub enumerated: bool,
}

#[cfg(test)]
mod tests {
    use alloc::string::String;

    use crate::collection::ReplicaCollectionId;

    #[test]
    fn id_converts_from_owned_and_borrowed_strings() {
        let owned = ReplicaCollectionId::from(String::from("inbox"));
        let borrowed = ReplicaCollectionId::from("inbox");
        assert_eq!(owned, borrowed);
        assert_eq!(owned.as_str(), "inbox");
    }
}
