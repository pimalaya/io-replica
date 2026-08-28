//! I/O-free coroutine to open a collection fully offline.
//!
//! A single storage read: load the placements and checkpoint, hand them
//! straight back. No network is ever touched.

use log::{debug, trace};

use crate::{
    collection::ReplicaCollectionId,
    coroutine::*,
    storage::{ReplicaLoadScope, ReplicaLoaded},
};

/// I/O-free OPEN coroutine.
pub struct ReplicaOpen {
    collection: ReplicaCollectionId,
    state: State,
}

impl ReplicaOpen {
    /// Creates a coroutine that loads `collection` from storage.
    pub fn new(collection: impl Into<ReplicaCollectionId>) -> Self {
        let collection = collection.into();
        debug!("open collection {}", collection.as_str());

        Self {
            collection,
            state: State::Start,
        }
    }
}

impl ReplicaCoroutine for ReplicaOpen {
    type Yield = ReplicaYield;
    type Return = Result<ReplicaLoaded, ReplicaArgError>;

    fn resume(
        &mut self,
        arg: Option<ReplicaArg>,
    ) -> ReplicaCoroutineState<Self::Yield, Self::Return> {
        match (&self.state, arg) {
            (State::Start, None) => {
                debug!("load collection from storage");
                self.state = State::PendingLoad;
                ReplicaCoroutineState::Yielded(ReplicaYield::WantsLoad {
                    collection: self.collection.clone(),
                    scope: ReplicaLoadScope::All,
                })
            }
            (State::PendingLoad, Some(ReplicaArg::Load(loaded))) => {
                debug!("opened collection with {} items", loaded.placements.len());
                trace!("loaded placements: {:?}", loaded.placements);
                // NOTE: a completed coroutine stays completed, or a
                // resumed run would hand back an empty success a caller
                // cannot tell from one that did nothing.
                self.state = State::Done;
                ReplicaCoroutineState::Complete(Ok(loaded))
            }
            (_, Some(_)) => ReplicaCoroutineState::Complete(Err(ReplicaArgError::UnexpectedArg)),
            (_, None) => ReplicaCoroutineState::Complete(Err(ReplicaArgError::MissingArg)),
        }
    }
}

enum State {
    Start,
    PendingLoad,
    Done,
}

#[cfg(test)]
mod tests {
    use alloc::vec;

    use crate::{
        collection::ReplicaCheckpoint,
        open::*,
        placement::{ReplicaFlags, ReplicaHandle, ReplicaLevel, ReplicaPlacement, ReplicaStatus},
    };

    fn placement(handle: &str) -> ReplicaPlacement {
        ReplicaPlacement {
            sort_key: Default::default(),
            collection: "inbox".into(),
            handle: ReplicaHandle::from(handle),
            link_id: None,
            object: None,
            level: ReplicaLevel::Probed,
            meta: None,
            flags: ReplicaFlags::default(),
            status: ReplicaStatus::Clean,
            conflict_revision: None,
            base: None,
            origin: None,
        }
    }

    #[test]
    fn start_yields_load() {
        let mut open = ReplicaOpen::new("inbox");
        match open.resume(None) {
            ReplicaCoroutineState::Yielded(ReplicaYield::WantsLoad { collection, .. }) => {
                assert_eq!(collection.as_str(), "inbox");
            }
            state => panic!("expected WantsLoad, got {state:?}"),
        }
    }

    #[test]
    fn load_completes_with_placements() {
        crate::testlog::init();
        let mut open = ReplicaOpen::new("inbox");
        let _ = open.resume(None);

        let loaded = ReplicaLoaded {
            placements: vec![placement("1"), placement("2")],
            checkpoint: Some(ReplicaCheckpoint(b"tok".to_vec())),
        };
        match open.resume(Some(ReplicaArg::Load(loaded))) {
            ReplicaCoroutineState::Complete(Ok(out)) => assert_eq!(out.placements.len(), 2),
            state => panic!("expected Complete(Ok), got {state:?}"),
        }
    }

    /// An empty success is indistinguishable from a run that did
    /// nothing, so a driver resuming a finished coroutine must be told.
    #[test]
    fn a_completed_open_does_not_resume() {
        let mut open = ReplicaOpen::new("inbox");
        let _ = open.resume(None);
        let _ = open.resume(Some(ReplicaArg::Load(ReplicaLoaded {
            placements: vec![placement("1")],
            checkpoint: None,
        })));

        match open.resume(Some(ReplicaArg::Write)) {
            ReplicaCoroutineState::Complete(Err(ReplicaArgError::UnexpectedArg)) => {}
            state => panic!("expected UnexpectedArg, got {state:?}"),
        }
    }

    #[test]
    fn unexpected_arg_at_start_errors() {
        let mut open = ReplicaOpen::new("inbox");
        match open.resume(Some(ReplicaArg::Write)) {
            ReplicaCoroutineState::Complete(Err(ReplicaArgError::UnexpectedArg)) => {}
            state => panic!("expected UnexpectedArg, got {state:?}"),
        }
    }

    #[test]
    fn missing_arg_at_pending_load_errors() {
        let mut open = ReplicaOpen::new("inbox");
        let _ = open.resume(None);
        match open.resume(None) {
            ReplicaCoroutineState::Complete(Err(ReplicaArgError::MissingArg)) => {}
            state => panic!("expected MissingArg, got {state:?}"),
        }
    }

    #[test]
    fn wrong_arg_kind_at_pending_load_errors() {
        let mut open = ReplicaOpen::new("inbox");
        let _ = open.resume(None);
        match open.resume(Some(ReplicaArg::Write)) {
            ReplicaCoroutineState::Complete(Err(ReplicaArgError::UnexpectedArg)) => {}
            state => panic!("expected UnexpectedArg, got {state:?}"),
        }
    }
}
