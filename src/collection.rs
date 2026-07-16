//! A mailbox, address book or calendar: a set of placements plus a sync
//! checkpoint.

use alloc::{string::String, vec::Vec};

/// The account-scoped identity of a collection.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Hash)]
pub struct OfflineCollectionId(pub String);

impl OfflineCollectionId {
    /// Borrows the id as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<&str> for OfflineCollectionId {
    fn from(value: &str) -> Self {
        Self(value.into())
    }
}

impl From<String> for OfflineCollectionId {
    fn from(value: String) -> Self {
        Self(value)
    }
}

/// An opaque per-collection sync token.
///
/// A QRESYNC pack, a JMAP state string, or a WebDAV sync-token; the engine
/// never inspects it, it only round-trips it between storage and remote.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct OfflineCheckpoint(pub Vec<u8>);

/// A collection's metadata, independent of its placements.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OfflineCollection {
    /// The account-scoped id.
    pub id: OfflineCollectionId,
    /// The human-facing name.
    pub name: String,
    /// The last sync checkpoint, if ever synced.
    pub checkpoint: Option<OfflineCheckpoint>,
    /// Whether the probed spine is complete as of the checkpoint.
    pub enumerated: bool,
}

#[cfg(test)]
mod tests {
    use alloc::string::String;

    use crate::collection::OfflineCollectionId;

    #[test]
    fn id_converts_from_owned_and_borrowed_strings() {
        let owned = OfflineCollectionId::from(String::from("inbox"));
        let borrowed = OfflineCollectionId::from("inbox");
        assert_eq!(owned, borrowed);
        assert_eq!(owned.as_str(), "inbox");
    }
}
