//! The logical item content, content-addressed and stored once.
//!
//! An object is one of the two identity axes (see [`crate::placement`] for
//! the other). Where an item's content is immutable the object never
//! changes; where it is mutable an edit is simply a new object (new hash)
//! and the old one is dereferenced. Many placements may point at one
//! object: this is the dedup and unified-view mechanism.

use alloc::string::String;

/// The byte-exact identity of an object, naming it in the object store.
///
/// A collision-resistant content hash (for example truncated SHA-256).
/// Size is a valid cheap pre-check only: identical bytes have identical
/// size, but equal size does not imply equal bytes.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Hash)]
pub struct Hash(pub String);

impl Hash {
    /// Borrows the hash as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<&str> for Hash {
    fn from(value: &str) -> Self {
        Self(value.into())
    }
}

impl From<String> for Hash {
    fn from(value: String) -> Self {
        Self(value)
    }
}

/// A stored object: its content hash and byte size.
///
/// The bytes live out of band at `blobdir/<hash>`; the store keeps a
/// refcount so copy, move and undelete are reference edits.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Object {
    /// The content hash naming the bytes.
    pub hash: Hash,
    /// The byte size of the content.
    pub size: usize,
}
