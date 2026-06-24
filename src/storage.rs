//! Payloads exchanged with the storage seam.
//!
//! Symmetric to [`crate::remote`]: the consumer satisfies storage by
//! reading and writing its index plus blob store (sqlite plus a blob dir
//! in the reference store). These types are what the read capabilities
//! return; writes travel as [`crate::change::WriteOp`].

use alloc::vec::Vec;

use crate::{collection::Checkpoint, placement::Placement};

/// A loaded collection: its placements and its last checkpoint.
///
/// The reply to a load; handed straight to the UI for a fully offline
/// open.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Loaded {
    /// Every placement currently stored for the collection.
    pub placements: Vec<Placement>,
    /// The last sync checkpoint, if ever synced.
    pub checkpoint: Option<Checkpoint>,
}
