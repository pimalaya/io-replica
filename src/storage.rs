//! Payloads exchanged with the storage seam.
//!
//! Symmetric to [`crate::remote`]: the consumer satisfies storage by
//! reading and writing its index plus blob store (sqlite plus a blob dir
//! in the reference store). These types are what the read capabilities
//! return; writes travel as [`crate::change::ReplicaWriteOp`].

use alloc::vec::Vec;

use crate::{
    collection::ReplicaCheckpoint,
    placement::{ReplicaHandle, ReplicaLinkId, ReplicaPlacement},
};

/// Which of a collection's placements a load has to return.
///
/// Most verbs read a handful of rows and merge nothing: a mutation reads
/// the one placement it edits, an upgrade the ones it raises. Only the
/// merge and the rebuild need the whole collection, because only they
/// reason about what is *missing* from it.
///
/// A scope is a floor, not a ceiling: a storage SHALL return at least the
/// placements named here and MAY return more, so returning the whole
/// collection is always correct, just as expensive as it sounds. Under-
/// delivering is not: a mutation that cannot see a colliding link id
/// creates a duplicate.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ReplicaLoadScope {
    /// Every placement of the collection.
    All,
    /// The placements holding these handles.
    Handles(Vec<ReplicaHandle>),
    /// Every placement holding one of these link ids, however many rows that
    /// is: the reads that ask about an identity rather than about a location,
    /// and so have to see every row that claims it.
    Links(Vec<ReplicaLinkId>),
}

/// A loaded collection: its placements and its last checkpoint.
///
/// The reply to a load; handed straight to the UI for a fully offline
/// open.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ReplicaLoaded {
    /// Every placement currently stored for the collection.
    pub placements: Vec<ReplicaPlacement>,
    /// The last sync checkpoint, if ever synced.
    pub checkpoint: Option<ReplicaCheckpoint>,
}
