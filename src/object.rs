//! The logical item content, content-addressed and stored once.
//!
//! An object is one of the two identity axes (see [`crate::placement`] for
//! the other). Where an item's content is immutable the object never
//! changes; where it is mutable an edit is simply a new object (new hash)
//! and the old one is dereferenced. Many placements may point at one
//! object: this is the dedup and unified-view mechanism.

crate::replica_id! {
    /// The byte-exact identity of an object, naming it in the object store.
    ///
    /// A collision-resistant content hash (for example truncated SHA-256).
    /// Size is a valid cheap pre-check only: identical bytes have identical
    /// size, but equal size does not imply equal bytes.
    ReplicaHash, Ord, PartialOrd, Hash,
}

/// A stored object: its content hash and byte size.
///
/// The bytes live out of band at `blobdir/<hash>`; the store keeps a
/// refcount so copy, move and undelete are reference edits.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReplicaObject {
    /// The content hash naming the bytes.
    pub hash: ReplicaHash,
    /// The byte size of the content.
    pub size: usize,
}

#[cfg(test)]
mod tests {
    use alloc::string::String;

    use crate::object::ReplicaHash;

    #[test]
    fn hash_converts_from_owned_and_borrowed_strings() {
        let owned = ReplicaHash::from(String::from("abc"));
        let borrowed = ReplicaHash::from("abc");
        assert_eq!(owned, borrowed);
        assert_eq!(owned.as_str(), "abc");
    }
}
