//! A mailbox, address book or calendar: the id the engine scopes every
//! verb to, and the opaque token it round-trips between the two seams.

use alloc::vec::Vec;

crate::replica_id! {
    /// The account-scoped identity of a collection.
    ReplicaCollectionId, Ord, PartialOrd, Hash,
}

/// An opaque per-collection sync token.
///
/// A QRESYNC pack, a JMAP state string or a WebDAV sync-token, never
/// inspected by the engine, only round-tripped between the two seams.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ReplicaCheckpoint(pub Vec<u8>);

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
