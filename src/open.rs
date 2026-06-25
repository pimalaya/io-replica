//! I/O-free coroutine to open a collection fully offline.
//!
//! A single storage read: load the placements and checkpoint, hand them
//! straight back. No network is ever touched.

use core::fmt;

use log::{debug, trace};
use thiserror::Error;

use crate::{collection::CollectionId, coroutine::*, storage::Loaded};

/// Failure causes during an OPEN flow.
#[derive(Clone, Debug, Error)]
pub enum OfflineOpenError {
    /// The driver fed back an arg that does not match the pending yield.
    #[error("Offline OPEN failed: unexpected coroutine arg")]
    UnexpectedArg,
    /// The driver resumed without the arg the pending yield required.
    #[error("Offline OPEN failed: missing coroutine arg")]
    MissingArg,
}

/// I/O-free OPEN coroutine.
pub struct OfflineOpen {
    collection: CollectionId,
    state: State,
}

impl OfflineOpen {
    /// Creates a coroutine that loads `collection` from storage.
    pub fn new(collection: impl Into<CollectionId>) -> Self {
        let collection = collection.into();
        debug!("open collection {}", collection.as_str());

        Self {
            collection,
            state: State::Start,
        }
    }
}

impl OfflineCoroutine for OfflineOpen {
    type Yield = OfflineYield;
    type Return = Result<Loaded, OfflineOpenError>;

    fn resume(
        &mut self,
        arg: Option<OfflineArg>,
    ) -> OfflineCoroutineState<Self::Yield, Self::Return> {
        match (&self.state, arg) {
            (State::Start, None) => {
                debug!("load collection from storage");
                self.state = State::PendingLoad;
                OfflineCoroutineState::Yielded(OfflineYield::WantsLoad(self.collection.clone()))
            }
            (State::PendingLoad, Some(OfflineArg::Load(loaded))) => {
                debug!("opened collection with {} items", loaded.placements.len());
                trace!("loaded placements: {:?}", loaded.placements);
                OfflineCoroutineState::Complete(Ok(loaded))
            }
            (_, Some(_)) => OfflineCoroutineState::Complete(Err(OfflineOpenError::UnexpectedArg)),
            (_, None) => OfflineCoroutineState::Complete(Err(OfflineOpenError::MissingArg)),
        }
    }
}

enum State {
    Start,
    PendingLoad,
}

impl fmt::Display for State {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Start => f.write_str("start"),
            Self::PendingLoad => f.write_str("pending load"),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        collection::Checkpoint,
        open::*,
        placement::{Flags, Handle, Level, Placement, Status},
    };

    fn placement(handle: &str) -> Placement {
        Placement {
            collection: "inbox".into(),
            handle: Handle::from(handle),
            link_id: None,
            object: None,
            level: Level::Probed,
            meta: None,
            flags: Flags::default(),
            status: Status::Clean,
            base: None,
            origin: None,
        }
    }

    #[test]
    fn start_yields_load() {
        let mut open = OfflineOpen::new("inbox");
        match open.resume(None) {
            OfflineCoroutineState::Yielded(OfflineYield::WantsLoad(id)) => {
                assert_eq!(id.as_str(), "inbox");
            }
            state => panic!("expected WantsLoad, got {state:?}"),
        }
    }

    #[test]
    fn load_completes_with_placements() {
        let mut open = OfflineOpen::new("inbox");
        let _ = open.resume(None);

        let loaded = Loaded {
            placements: vec![placement("1"), placement("2")],
            checkpoint: Some(Checkpoint(b"tok".to_vec())),
        };
        match open.resume(Some(OfflineArg::Load(loaded))) {
            OfflineCoroutineState::Complete(Ok(out)) => assert_eq!(out.placements.len(), 2),
            state => panic!("expected Complete(Ok), got {state:?}"),
        }
    }

    #[test]
    fn unexpected_arg_at_start_errors() {
        let mut open = OfflineOpen::new("inbox");
        match open.resume(Some(OfflineArg::Write)) {
            OfflineCoroutineState::Complete(Err(OfflineOpenError::UnexpectedArg)) => {}
            state => panic!("expected UnexpectedArg, got {state:?}"),
        }
    }

    #[test]
    fn missing_arg_at_pending_load_errors() {
        let mut open = OfflineOpen::new("inbox");
        let _ = open.resume(None);
        match open.resume(None) {
            OfflineCoroutineState::Complete(Err(OfflineOpenError::MissingArg)) => {}
            state => panic!("expected MissingArg, got {state:?}"),
        }
    }

    #[test]
    fn wrong_arg_kind_at_pending_load_errors() {
        let mut open = OfflineOpen::new("inbox");
        let _ = open.resume(None);
        match open.resume(Some(OfflineArg::Count(0))) {
            OfflineCoroutineState::Complete(Err(OfflineOpenError::UnexpectedArg)) => {}
            state => panic!("expected UnexpectedArg, got {state:?}"),
        }
    }
}
