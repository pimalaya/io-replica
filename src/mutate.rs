//! I/O-free coroutine to mutate a collection locally, with no network.
//!
//! Loads the target placement, applies the change in memory, marks it
//! dirty or tombstone, and writes it back. The base is left untouched so
//! the next sync derives the pending push, the one exception being a
//! resolution, which rebases it onto the remote state it settled;
//! reconciliation is [`crate::sync`]'s job.

use alloc::{string::String, vec, vec::Vec};

use log::{debug, trace};
use thiserror::Error;

use crate::{
    change::ReplicaWriteOp,
    collection::ReplicaCollectionId,
    coroutine::*,
    object::ReplicaObject,
    placement::{
        ReplicaBase, ReplicaFlags, ReplicaHandle, ReplicaLevel, ReplicaLinkId, ReplicaMeta,
        ReplicaOrigin, ReplicaPlacement, ReplicaSortKey, ReplicaStatus,
    },
    storage::ReplicaLoadScope,
};

/// A local edit applied offline.
///
/// Each mutation reads one source placement in the coroutine's
/// collection and stages the resulting writes, to be reconciled on the
/// next sync. A copy stages a [`ReplicaStatus::Created`] placement in
/// another collection.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ReplicaMutation {
    /// Replace a placement's flag set.
    SetFlags {
        /// The placement to update.
        handle: ReplicaHandle,
        /// The new flag set.
        flags: ReplicaFlags,
    },
    /// Mark a placement deleted, keeping it as a tombstone until synced.
    Remove(ReplicaHandle),
    /// Replace a placement's body with locally edited content: the new
    /// object is stored and the placement repointed at it and marked
    /// dirty, its base keeping the previously synced body so the next
    /// sync derives the push.
    ///
    /// Editing a conflicted placement resolves it, and the base becomes
    /// the remote state the resolution was merged against: the revision
    /// observed at conflict time and the body recorded beside it. The
    /// resolving push is thus conditioned on that revision, and measured
    /// against that body, so keeping the ancestor of the divergence is a
    /// decision the remote hears like any other.
    Edit {
        /// The placement to update.
        handle: ReplicaHandle,
        /// The new body's object metadata.
        object: ReplicaObject,
        /// The new body bytes.
        body: Vec<u8>,
        /// The refreshed summary, when the consumer projects one; `None`
        /// keeps the cached summary.
        meta: Option<ReplicaMeta>,
        /// The refreshed sort key, on the same terms as `meta`. An edit
        /// changing what a key is derived from has to say so, or the
        /// item stays where it was in the list.
        sort_key: Option<ReplicaSortKey>,
    },
    /// Copy a placement into `target` as a pending create that the next
    /// sync pushes with a server-side copy (no body re-upload). The source
    /// is left untouched.
    Copy {
        /// The source placement to copy.
        handle: ReplicaHandle,
        /// The collection to copy it into.
        target: ReplicaCollectionId,
        /// The provisional handle the copy is staged under in `target`.
        placeholder: ReplicaHandle,
    },
    /// Move a placement into `target`: stage a `Created` placement there
    /// under `placeholder`, carrying the source origin, and tombstone
    /// the source carrying `target` as its destination. Each
    /// collection's next sync derives one half and whichever runs first
    /// delivers the item, the link id keeping the other from delivering
    /// a second copy (see
    /// [`ReplicaChange`](crate::change::ReplicaChange)).
    ///
    /// An item whose link id is not resolved yet has no such key, so
    /// only the source half is staged: the relocation delivers it alone,
    /// and the target picks it up on its next enumerate.
    Move {
        /// The source placement to move.
        handle: ReplicaHandle,
        /// The collection to move it into.
        target: ReplicaCollectionId,
        /// The provisional handle the move is staged under in `target`.
        placeholder: ReplicaHandle,
    },
    /// Create a brand-new, locally-authored item with no remote origin
    /// (compose, import). Stages a pending create the next sync pushes
    /// as an append, uploading the body, rather than a server-side copy.
    /// Reads no existing source.
    Add {
        /// The provisional local handle the create is staged under,
        /// rekeyed to the server-assigned handle when the push reports
        /// it.
        handle: ReplicaHandle,
        /// The item's cross-source link id (its `Message-ID`, …).
        link_id: ReplicaLinkId,
        /// The initial flag set.
        flags: ReplicaFlags,
        /// The new body's object metadata.
        object: ReplicaObject,
        /// The new body bytes.
        body: Vec<u8>,
        /// The summary, when the consumer projects one.
        meta: Option<ReplicaMeta>,
        /// The sort key, when the consumer's kind defines one.
        sort_key: ReplicaSortKey,
    },
}

impl ReplicaMutation {
    /// The source handle the mutation reads, or `None` for
    /// [`Add`](Self::Add), which creates a placement rather than reading
    /// one.
    fn handle(&self) -> Option<&ReplicaHandle> {
        match self {
            Self::SetFlags { handle, .. } => Some(handle),
            Self::Remove(handle) => Some(handle),
            Self::Edit { handle, .. } => Some(handle),
            Self::Copy { handle, .. } => Some(handle),
            Self::Move { handle, .. } => Some(handle),
            Self::Add { .. } => None,
        }
    }

    /// What the mutation has to read: the one placement it edits, or, for
    /// an [`Add`](Self::Add), every row holding the link id it must not
    /// collide with.
    fn scope(&self) -> ReplicaLoadScope {
        match self {
            Self::Add { link_id, .. } => ReplicaLoadScope::Links(alloc::vec![link_id.clone()]),
            other => match other.handle() {
                Some(handle) => ReplicaLoadScope::Handles(alloc::vec![handle.clone()]),
                None => ReplicaLoadScope::All,
            },
        }
    }
}

/// Failure causes during a MUTATE flow.
#[derive(Clone, Debug, Error)]
pub enum ReplicaMutateError {
    /// The targeted handle has no placement in the collection.
    #[error("Replica MUTATE failed: unknown handle {0}")]
    UnknownHandle(String),
    /// An `Add` names a link id a live placement already holds.
    #[error("Replica MUTATE failed: link id already present: {0}")]
    LinkExists(String),
    /// The driver broke the coroutine contract.
    #[error(transparent)]
    Arg(#[from] ReplicaArgError),
}

/// I/O-free MUTATE coroutine.
pub struct ReplicaMutate {
    collection: ReplicaCollectionId,
    mutation: ReplicaMutation,
    state: State,
}

impl ReplicaMutate {
    /// Creates a coroutine that applies `mutation` to `collection`.
    pub fn new(collection: impl Into<ReplicaCollectionId>, mutation: ReplicaMutation) -> Self {
        let collection = collection.into();
        debug!("mutate collection {}", collection.as_str());

        Self {
            collection,
            mutation,
            state: State::Start,
        }
    }

    /// Stages the writes for the mutation given its loaded source
    /// placement. Flag sets and removes rewrite the source in place; a
    /// copy leaves it untouched and stages a pending create in the
    /// target.
    fn writes(&self, mut source: ReplicaPlacement) -> Vec<ReplicaWriteOp> {
        match &self.mutation {
            ReplicaMutation::SetFlags { flags, .. } => {
                source.flags = flags.clone();
                // NOTE: a pending create stays a create and an
                // unresolved conflict stays a conflict, its resolution
                // being an edit; the flag change rides along either way.
                if source.status == ReplicaStatus::Clean {
                    source.status = ReplicaStatus::Dirty;
                }
                vec![ReplicaWriteOp::UpsertPlacement(source)]
            }
            ReplicaMutation::Remove(_) => {
                source.status = ReplicaStatus::Tombstone;
                vec![ReplicaWriteOp::UpsertPlacement(source)]
            }
            ReplicaMutation::Edit {
                object,
                body,
                meta,
                sort_key,
                ..
            } => {
                source.object = Some(object.hash.clone());
                source.level = ReplicaLevel::Full;
                if meta.is_some() {
                    source.meta = meta.clone();
                }
                if let Some(sort_key) = sort_key {
                    source.sort_key = sort_key.clone();
                }

                // NOTE: editing a conflict is its resolution, and the
                // base becomes the remote state it was merged against,
                // both halves of it. Adopting the revision alone leaves
                // the pair contradicting itself, the base claiming a
                // revision its object was never the content of, and the
                // sync compares against the object: a resolution keeping
                // the ancestor would read as nothing to push. A conflict
                // with no base is given one, its own resolution being
                // where the two sides first agree.
                let resolving = source.status == ReplicaStatus::Conflict;
                if resolving {
                    let revision = source.conflict_revision.take();
                    let settled = source.conflict_object.take();
                    let base = source.base.get_or_insert_with(|| ReplicaBase {
                        flags: source.flags.clone(),
                        revision: None,
                        object: None,
                    });
                    base.revision = revision;
                    base.object = settled;
                }

                // NOTE: an edit restating the synced body stages nothing,
                // so it leaves the status where it found it: a placement
                // whose `staged_edit` reads `None` must not claim a
                // pending content push. A resolution is dirty either
                // way, the divergence it settles being the change.
                let staged = source
                    .base
                    .as_ref()
                    .is_none_or(|base| base.object.as_ref() != Some(&object.hash));
                if source.status != ReplicaStatus::Created && (staged || resolving) {
                    source.status = ReplicaStatus::Dirty;
                }

                vec![
                    ReplicaWriteOp::StoreObject {
                        object: object.clone(),
                        body: Some(body.clone()),
                    },
                    ReplicaWriteOp::UpsertPlacement(source),
                ]
            }
            ReplicaMutation::Copy {
                target,
                placeholder,
                ..
            } => {
                let create = Self::staged_copy(&source, target, placeholder);
                vec![ReplicaWriteOp::UpsertPlacement(create)]
            }
            ReplicaMutation::Move {
                target,
                placeholder,
                ..
            } => {
                // NOTE: a move is staged twice over and whichever half
                // reaches the server first delivers: the target's create
                // and the source's remove. Both carry the link id, so
                // the second half recognises what the first did instead
                // of delivering a second copy, the same at-least-once
                // discipline `ReplicaChange` states for a retried add.
                //
                // An item whose link id is unresolved has no such key,
                // so no create is staged: the relocation delivers it
                // alone and the target picks it up on its next
                // enumerate.
                let create = source
                    .link_id
                    .is_some()
                    .then(|| Self::staged_copy(&source, target, placeholder));

                source.status = ReplicaStatus::Tombstone;
                source.origin = Some(ReplicaOrigin {
                    collection: target.clone(),
                    handle: source.handle.clone(),
                });

                create
                    .map(ReplicaWriteOp::UpsertPlacement)
                    .into_iter()
                    .chain([ReplicaWriteOp::UpsertPlacement(source)])
                    .collect()
            }
            ReplicaMutation::Add { .. } => self.create_writes(),
        }
    }

    /// The `Created` placement a copy or a move stages in its target,
    /// carrying the source as its [`ReplicaOrigin`] so the push is a
    /// server-side copy rather than a body upload.
    fn staged_copy(
        source: &ReplicaPlacement,
        target: &ReplicaCollectionId,
        placeholder: &ReplicaHandle,
    ) -> ReplicaPlacement {
        let origin = Some(ReplicaOrigin {
            collection: source.collection.clone(),
            handle: source.handle.clone(),
        });

        ReplicaPlacement {
            collection: target.clone(),
            handle: placeholder.clone(),
            link_id: source.link_id.clone(),
            object: source.object.clone(),
            level: source.level,
            meta: source.meta.clone(),
            sort_key: source.sort_key.clone(),
            flags: source.flags.clone(),
            status: ReplicaStatus::Created,
            conflict_revision: None,
            conflict_object: None,
            base: None,
            origin,
        }
    }

    /// Stages the writes for an [`Add`](ReplicaMutation::Add): a
    /// locally-authored `Created` placement with no base and no origin,
    /// so the next sync appends it rather than server-copying, plus its
    /// object.
    fn create_writes(&self) -> Vec<ReplicaWriteOp> {
        let ReplicaMutation::Add {
            handle,
            link_id,
            flags,
            object,
            body,
            meta,
            sort_key,
        } = &self.mutation
        else {
            return Vec::new();
        };

        let create = ReplicaPlacement {
            collection: self.collection.clone(),
            handle: handle.clone(),
            link_id: Some(link_id.clone()),
            object: Some(object.hash.clone()),
            level: ReplicaLevel::Full,
            meta: meta.clone(),
            sort_key: sort_key.clone(),
            flags: flags.clone(),
            status: ReplicaStatus::Created,
            conflict_revision: None,
            conflict_object: None,
            base: None,
            origin: None,
        };
        vec![
            ReplicaWriteOp::StoreObject {
                object: object.clone(),
                body: Some(body.clone()),
            },
            ReplicaWriteOp::UpsertPlacement(create),
        ]
    }
}

impl ReplicaCoroutine for ReplicaMutate {
    type Yield = ReplicaYield;
    type Return = Result<(), ReplicaMutateError>;

    fn resume(
        &mut self,
        arg: Option<ReplicaArg>,
    ) -> ReplicaCoroutineState<Self::Yield, Self::Return> {
        match (&mut self.state, arg) {
            (State::Start, None) => {
                debug!("load target item from storage");
                self.state = State::PendingLoad;
                ReplicaCoroutineState::Yielded(ReplicaYield::WantsLoad {
                    collection: self.collection.clone(),
                    scope: self.mutation.scope(),
                })
            }
            (State::PendingLoad, Some(ReplicaArg::Load(loaded))) => {
                let ops = if let ReplicaMutation::Add { link_id, .. } = &self.mutation {
                    // NOTE: no source to read, so guard against
                    // re-creating a live item.
                    let collides = loaded.placements.iter().any(|p| {
                        p.status != ReplicaStatus::Tombstone && p.link_id.as_ref() == Some(link_id)
                    });
                    if collides {
                        let err = ReplicaMutateError::LinkExists(link_id.0.clone());
                        return ReplicaCoroutineState::Complete(Err(err));
                    }
                    self.create_writes()
                } else {
                    let handle = self
                        .mutation
                        .handle()
                        .expect("non-Add mutation has a handle");
                    let Some(placement) =
                        loaded.placements.into_iter().find(|p| &p.handle == handle)
                    else {
                        let err = ReplicaMutateError::UnknownHandle(handle.as_str().into());
                        return ReplicaCoroutineState::Complete(Err(err));
                    };
                    self.writes(placement)
                };

                debug!("stage local change, {} write(s)", ops.len());
                trace!("writes: {ops:?}");

                self.state = State::PendingWrite;
                ReplicaCoroutineState::Yielded(ReplicaYield::WantsWrite(ops))
            }
            (State::PendingWrite, Some(ReplicaArg::Write)) => {
                debug!("local change written");
                // NOTE: a completed coroutine stays completed: resuming
                // one is a driver bug, not an empty success.
                self.state = State::Done;
                ReplicaCoroutineState::Complete(Ok(()))
            }
            (_, Some(_)) => {
                ReplicaCoroutineState::Complete(Err(ReplicaArgError::UnexpectedArg.into()))
            }
            (_, None) => ReplicaCoroutineState::Complete(Err(ReplicaArgError::MissingArg.into())),
        }
    }
}

enum State {
    Start,
    PendingLoad,
    PendingWrite,
    Done,
}

#[cfg(test)]
mod tests {
    use alloc::vec;

    use crate::{
        mutate::*,
        object::ReplicaHash,
        placement::{ReplicaBase, ReplicaLevel, ReplicaStatus},
        storage::ReplicaLoaded,
    };

    fn loaded(handle: &str) -> ReplicaLoaded {
        crate::testlog::init();
        ReplicaLoaded {
            placements: vec![ReplicaPlacement {
                sort_key: Default::default(),
                collection: "inbox".into(),
                handle: ReplicaHandle::from(handle),
                link_id: Some(ReplicaLinkId::from(handle)),
                object: None,
                level: ReplicaLevel::Meta,
                meta: None,
                flags: ReplicaFlags::default(),
                conflict_revision: None,
                conflict_object: None,
                status: ReplicaStatus::Clean,
                base: Some(ReplicaBase {
                    flags: ReplicaFlags::default(),
                    revision: None,
                    object: None,
                }),
                origin: None,
            }],
            checkpoint: None,
        }
    }

    #[test]
    fn set_flags_marks_dirty() {
        let mutation = ReplicaMutation::SetFlags {
            handle: ReplicaHandle::from("1"),
            flags: ReplicaFlags::from_iter(["seen"]),
        };
        let mut mutate = ReplicaMutate::new("inbox", mutation);
        let _ = mutate.resume(None);

        let ops = match mutate.resume(Some(ReplicaArg::Load(loaded("1")))) {
            ReplicaCoroutineState::Yielded(ReplicaYield::WantsWrite(ops)) => ops,
            state => panic!("expected WantsWrite, got {state:?}"),
        };
        let ReplicaWriteOp::UpsertPlacement(p) = &ops[0] else {
            panic!("expected UpsertPlacement, got {:?}", ops[0]);
        };
        assert_eq!(p.status, ReplicaStatus::Dirty);
        assert!(p.flags.contains("seen"));
        assert!(p.base.is_some(), "base must be preserved for sync");
    }

    #[test]
    fn set_flags_on_a_conflicted_placement_keeps_the_conflict() {
        // the flag edit rides along while the conflict stays unresolved,
        // so the sync never mistakes the placement for a plain dirty one
        let mutation = ReplicaMutation::SetFlags {
            handle: ReplicaHandle::from("1"),
            flags: ReplicaFlags::from_iter(["seen"]),
        };
        let mut mutate = ReplicaMutate::new("inbox", mutation);
        let _ = mutate.resume(None);

        let mut loaded = loaded("1");
        loaded.placements[0].status = ReplicaStatus::Conflict;
        loaded.placements[0].conflict_revision = Some("r2".into());

        let ops = match mutate.resume(Some(ReplicaArg::Load(loaded))) {
            ReplicaCoroutineState::Yielded(ReplicaYield::WantsWrite(ops)) => ops,
            state => panic!("expected WantsWrite, got {state:?}"),
        };
        let ReplicaWriteOp::UpsertPlacement(p) = &ops[0] else {
            panic!("expected UpsertPlacement, got {:?}", ops[0]);
        };
        assert_eq!(p.status, ReplicaStatus::Conflict);
        assert_eq!(p.conflict_revision.as_deref(), Some("r2"));
        assert!(p.flags.contains("seen"));
    }

    #[test]
    fn set_flags_on_a_created_placement_stays_created() {
        // a pending create keeps its status, else the sync would never
        // push the add
        let mutation = ReplicaMutation::SetFlags {
            handle: ReplicaHandle::from("1"),
            flags: ReplicaFlags::from_iter(["seen"]),
        };
        let mut mutate = ReplicaMutate::new("inbox", mutation);
        let _ = mutate.resume(None);

        let mut loaded = loaded("1");
        loaded.placements[0].status = ReplicaStatus::Created;
        loaded.placements[0].base = None;

        let ops = match mutate.resume(Some(ReplicaArg::Load(loaded))) {
            ReplicaCoroutineState::Yielded(ReplicaYield::WantsWrite(ops)) => ops,
            state => panic!("expected WantsWrite, got {state:?}"),
        };
        let ReplicaWriteOp::UpsertPlacement(p) = &ops[0] else {
            panic!("expected UpsertPlacement, got {:?}", ops[0]);
        };
        assert_eq!(p.status, ReplicaStatus::Created);
        assert!(p.flags.contains("seen"));
    }

    #[test]
    fn remove_marks_tombstone() {
        let mutation = ReplicaMutation::Remove(ReplicaHandle::from("1"));
        let mut mutate = ReplicaMutate::new("inbox", mutation);
        let _ = mutate.resume(None);

        let ops = match mutate.resume(Some(ReplicaArg::Load(loaded("1")))) {
            ReplicaCoroutineState::Yielded(ReplicaYield::WantsWrite(ops)) => ops,
            state => panic!("expected WantsWrite, got {state:?}"),
        };
        let ReplicaWriteOp::UpsertPlacement(p) = &ops[0] else {
            panic!("expected UpsertPlacement, got {:?}", ops[0]);
        };
        assert_eq!(p.status, ReplicaStatus::Tombstone);
    }

    #[test]
    fn unknown_handle_errors() {
        let mutation = ReplicaMutation::Remove(ReplicaHandle::from("nope"));
        let mut mutate = ReplicaMutate::new("inbox", mutation);
        let _ = mutate.resume(None);

        match mutate.resume(Some(ReplicaArg::Load(loaded("1")))) {
            ReplicaCoroutineState::Complete(Err(ReplicaMutateError::UnknownHandle(h))) => {
                assert_eq!(h, "nope");
            }
            state => panic!("expected UnknownHandle, got {state:?}"),
        }
    }

    #[test]
    fn add_stages_an_append_create() {
        // no base and no origin, which is the shape the sync pushes as
        // an append rather than a server-side copy
        use crate::object::{ReplicaHash, ReplicaObject};

        let mutation = ReplicaMutation::Add {
            sort_key: Default::default(),
            handle: ReplicaHandle::from("draft-1"),
            link_id: ReplicaLinkId("mid:new".into()),
            flags: ReplicaFlags::from_iter(["\\Draft"]),
            object: ReplicaObject {
                hash: ReplicaHash::from("deadbeef"),
                size: 5,
            },
            body: b"hello".to_vec(),
            meta: Some(ReplicaMeta("{\"v\":1}".into())),
        };
        let mut mutate = ReplicaMutate::new("inbox", mutation);
        let _ = mutate.resume(None);

        // Add reads no source but still loads to guard collisions, and
        // the loaded item carries no link id
        let ops = match mutate.resume(Some(ReplicaArg::Load(loaded("other")))) {
            ReplicaCoroutineState::Yielded(ReplicaYield::WantsWrite(ops)) => ops,
            state => panic!("expected WantsWrite, got {state:?}"),
        };

        let ReplicaWriteOp::StoreObject { body, object } = &ops[0] else {
            panic!("expected StoreObject, got {:?}", ops[0]);
        };
        assert_eq!(body.as_deref(), Some(&b"hello"[..]));
        assert_eq!(object.hash, ReplicaHash::from("deadbeef"));

        let ReplicaWriteOp::UpsertPlacement(p) = &ops[1] else {
            panic!("expected UpsertPlacement, got {:?}", ops[1]);
        };
        assert_eq!(p.status, ReplicaStatus::Created);
        assert!(p.base.is_none(), "no prior sync");
        assert!(p.origin.is_none(), "an append, not a server copy");
        assert_eq!(p.link_id, Some(ReplicaLinkId("mid:new".into())));
        assert_eq!(p.level, ReplicaLevel::Full);
        assert!(p.flags.contains("\\Draft"));
    }

    #[test]
    fn add_rejects_a_live_link_id_collision() {
        use crate::object::{ReplicaHash, ReplicaObject};

        let mutation = ReplicaMutation::Add {
            sort_key: Default::default(),
            handle: ReplicaHandle::from("draft-1"),
            link_id: ReplicaLinkId("mid:dup".into()),
            flags: ReplicaFlags::default(),
            object: ReplicaObject {
                hash: ReplicaHash::from("deadbeef"),
                size: 1,
            },
            body: b"x".to_vec(),
            meta: None,
        };
        let mut mutate = ReplicaMutate::new("inbox", mutation);
        let _ = mutate.resume(None);

        // a live placement already holds mid:dup
        let mut loaded = loaded("existing");
        loaded.placements[0].link_id = Some(ReplicaLinkId("mid:dup".into()));

        match mutate.resume(Some(ReplicaArg::Load(loaded))) {
            ReplicaCoroutineState::Complete(Err(ReplicaMutateError::LinkExists(l))) => {
                assert_eq!(l, "mid:dup");
            }
            state => panic!("expected LinkExists, got {state:?}"),
        }
    }

    #[test]
    fn add_over_a_tombstone_link_id_is_allowed() {
        // the delete is in flight and the new item supersedes it, so a
        // tombstone does not block a re-create
        use crate::object::{ReplicaHash, ReplicaObject};

        let mutation = ReplicaMutation::Add {
            sort_key: Default::default(),
            handle: ReplicaHandle::from("draft-1"),
            link_id: ReplicaLinkId("mid:gone".into()),
            flags: ReplicaFlags::default(),
            object: ReplicaObject {
                hash: ReplicaHash::from("deadbeef"),
                size: 1,
            },
            body: b"x".to_vec(),
            meta: None,
        };
        let mut mutate = ReplicaMutate::new("inbox", mutation);
        let _ = mutate.resume(None);

        let mut loaded = loaded("existing");
        loaded.placements[0].link_id = Some(ReplicaLinkId("mid:gone".into()));
        loaded.placements[0].status = ReplicaStatus::Tombstone;

        match mutate.resume(Some(ReplicaArg::Load(loaded))) {
            ReplicaCoroutineState::Yielded(ReplicaYield::WantsWrite(_)) => {}
            state => panic!("expected WantsWrite, got {state:?}"),
        }
    }

    #[test]
    fn write_completes() {
        let mutation = ReplicaMutation::Remove(ReplicaHandle::from("1"));
        let mut mutate = ReplicaMutate::new("inbox", mutation);
        let _ = mutate.resume(None);
        let _ = mutate.resume(Some(ReplicaArg::Load(loaded("1"))));

        match mutate.resume(Some(ReplicaArg::Write)) {
            ReplicaCoroutineState::Complete(Ok(())) => {}
            state => panic!("expected Complete(Ok), got {state:?}"),
        }
    }

    #[test]
    fn missing_arg_errors() {
        let mutation = ReplicaMutation::Remove(ReplicaHandle::from("1"));
        let mut mutate = ReplicaMutate::new("inbox", mutation);
        let _ = mutate.resume(None);

        match mutate.resume(None) {
            ReplicaCoroutineState::Complete(Err(ReplicaMutateError::Arg(
                ReplicaArgError::MissingArg,
            ))) => {}
            state => panic!("expected MissingArg, got {state:?}"),
        }
    }

    /// An empty success is indistinguishable from a run that did
    /// nothing, so a driver resuming a finished coroutine must be told.
    #[test]
    fn a_completed_mutate_does_not_resume() {
        let mutation = ReplicaMutation::Remove(ReplicaHandle::from("1"));
        let mut mutate = ReplicaMutate::new("inbox", mutation);
        let _ = mutate.resume(None);
        let _ = mutate.resume(Some(ReplicaArg::Load(loaded("1"))));
        let _ = mutate.resume(Some(ReplicaArg::Write));

        match mutate.resume(Some(ReplicaArg::Write)) {
            ReplicaCoroutineState::Complete(Err(ReplicaMutateError::Arg(
                ReplicaArgError::UnexpectedArg,
            ))) => {}
            state => panic!("expected UnexpectedArg, got {state:?}"),
        }
    }

    #[test]
    fn unexpected_arg_errors() {
        let mutation = ReplicaMutation::Remove(ReplicaHandle::from("1"));
        let mut mutate = ReplicaMutate::new("inbox", mutation);
        let _ = mutate.resume(None);

        match mutate.resume(Some(ReplicaArg::Write)) {
            ReplicaCoroutineState::Complete(Err(ReplicaMutateError::Arg(
                ReplicaArgError::UnexpectedArg,
            ))) => {}
            state => panic!("expected UnexpectedArg, got {state:?}"),
        }
    }

    #[test]
    fn edit_stages_a_dirty_body() {
        // the base keeps the synced state, so the next sync derives the
        // push
        use crate::object::{ReplicaHash, ReplicaObject};

        let mutation = ReplicaMutation::Edit {
            sort_key: Default::default(),
            handle: ReplicaHandle::from("1"),
            object: ReplicaObject {
                hash: ReplicaHash::from("h2"),
                size: 4,
            },
            body: b"body".to_vec(),
            meta: None,
        };
        let mut mutate = ReplicaMutate::new("inbox", mutation);
        let _ = mutate.resume(None);

        let ops = match mutate.resume(Some(ReplicaArg::Load(loaded("1")))) {
            ReplicaCoroutineState::Yielded(ReplicaYield::WantsWrite(ops)) => ops,
            state => panic!("expected WantsWrite, got {state:?}"),
        };

        assert!(
            matches!(&ops[0], ReplicaWriteOp::StoreObject { object, .. } if object.hash == ReplicaHash::from("h2"))
        );
        let ReplicaWriteOp::UpsertPlacement(p) = &ops[1] else {
            panic!("expected UpsertPlacement, got {:?}", ops[1]);
        };
        assert_eq!(p.status, ReplicaStatus::Dirty);
        assert_eq!(p.object, Some(ReplicaHash::from("h2")));
        assert_eq!(p.level, ReplicaLevel::Full);
        assert!(p.base.is_some(), "base must be preserved for sync");
    }

    #[test]
    fn an_edit_restating_the_synced_body_stages_nothing() {
        // the placement already points at that body, so there is no push
        // to pend: a dirty here would be one `staged_edit` reads as
        // absent, and the status is the only thing left saying otherwise
        use crate::object::{ReplicaHash, ReplicaObject};

        let mutation = ReplicaMutation::Edit {
            sort_key: Default::default(),
            handle: ReplicaHandle::from("1"),
            object: ReplicaObject {
                hash: ReplicaHash::from("h1"),
                size: 4,
            },
            body: b"body".to_vec(),
            meta: None,
        };
        let mut mutate = ReplicaMutate::new("inbox", mutation);
        let _ = mutate.resume(None);

        let mut loaded = loaded("1");
        loaded.placements[0].object = Some(ReplicaHash::from("h1"));
        loaded.placements[0].level = ReplicaLevel::Full;
        loaded.placements[0].base.as_mut().expect("a base").object = Some(ReplicaHash::from("h1"));

        let ops = match mutate.resume(Some(ReplicaArg::Load(loaded))) {
            ReplicaCoroutineState::Yielded(ReplicaYield::WantsWrite(ops)) => ops,
            state => panic!("expected WantsWrite, got {state:?}"),
        };
        let ReplicaWriteOp::UpsertPlacement(p) = &ops[1] else {
            panic!("expected UpsertPlacement, got {:?}", ops[1]);
        };
        assert_eq!(p.status, ReplicaStatus::Clean);
        assert_eq!(p.staged_edit(), None, "the status agrees with the reading");
    }

    #[test]
    fn resolving_a_conflict_with_the_base_body_still_pushes() {
        // discarding both diverging bodies for the shared ancestor is a
        // decision the remote has to hear, so the resolution is dirty
        // even though it restates the base
        use crate::object::{ReplicaHash, ReplicaObject};

        let mutation = ReplicaMutation::Edit {
            sort_key: Default::default(),
            handle: ReplicaHandle::from("1"),
            object: ReplicaObject {
                hash: ReplicaHash::from("h1"),
                size: 4,
            },
            body: b"body".to_vec(),
            meta: None,
        };
        let mut mutate = ReplicaMutate::new("inbox", mutation);
        let _ = mutate.resume(None);

        let mut loaded = loaded("1");
        loaded.placements[0].status = ReplicaStatus::Conflict;
        loaded.placements[0].object = Some(ReplicaHash::from("h2"));
        loaded.placements[0].level = ReplicaLevel::Full;
        loaded.placements[0].conflict_revision = Some("r2".into());
        loaded.placements[0].conflict_object = Some(ReplicaHash::from("h-remote"));
        loaded.placements[0].base.as_mut().expect("a base").object = Some(ReplicaHash::from("h1"));

        let ops = match mutate.resume(Some(ReplicaArg::Load(loaded))) {
            ReplicaCoroutineState::Yielded(ReplicaYield::WantsWrite(ops)) => ops,
            state => panic!("expected WantsWrite, got {state:?}"),
        };
        let ReplicaWriteOp::UpsertPlacement(p) = &ops[1] else {
            panic!("expected UpsertPlacement, got {:?}", ops[1]);
        };
        assert_eq!(p.status, ReplicaStatus::Dirty);
        assert_eq!(p.conflict_revision, None);
        assert_eq!(p.conflict_object, None);
    }

    #[test]
    fn a_resolution_adopts_the_whole_remote_state_into_the_base() {
        // the base a resolution leaves is the remote state it was merged
        // against: half of it, the revision without the body, claims a
        // revision the base object was never the content of, and the
        // next sync then reads a resolution keeping the ancestor as
        // nothing to push
        use crate::object::{ReplicaHash, ReplicaObject};

        let mutation = ReplicaMutation::Edit {
            sort_key: Default::default(),
            handle: ReplicaHandle::from("1"),
            object: ReplicaObject {
                hash: ReplicaHash::from("h-base"),
                size: 4,
            },
            body: b"base".to_vec(),
            meta: None,
        };
        let mut mutate = ReplicaMutate::new("inbox", mutation);
        let _ = mutate.resume(None);

        let mut loaded = loaded("1");
        loaded.placements[0].status = ReplicaStatus::Conflict;
        loaded.placements[0].object = Some(ReplicaHash::from("h-local"));
        loaded.placements[0].level = ReplicaLevel::Full;
        loaded.placements[0].conflict_revision = Some("r2".into());
        loaded.placements[0].conflict_object = Some(ReplicaHash::from("h-remote"));
        let base = loaded.placements[0].base.as_mut().expect("a base");
        base.revision = Some("r1".into());
        base.object = Some(ReplicaHash::from("h-base"));

        let ops = match mutate.resume(Some(ReplicaArg::Load(loaded))) {
            ReplicaCoroutineState::Yielded(ReplicaYield::WantsWrite(ops)) => ops,
            state => panic!("expected WantsWrite, got {state:?}"),
        };
        let ReplicaWriteOp::UpsertPlacement(p) = &ops[1] else {
            panic!("expected UpsertPlacement, got {:?}", ops[1]);
        };
        let base = p.base.as_ref().expect("a base");
        assert_eq!(base.revision.as_deref(), Some("r2"));
        assert_eq!(
            base.object,
            Some(ReplicaHash::from("h-remote")),
            "the base object is the body the adopted revision names",
        );
        assert_eq!(
            p.staged_edit(),
            Some(&ReplicaHash::from("h-base")),
            "so the ancestor the resolution kept is a body to push",
        );
    }

    #[test]
    fn a_resolution_gives_a_base_less_conflict_a_base() {
        // a create-collision conflicts with no ancestor to merge
        // against, and a resolution leaving it base-less is re-marked
        // conflicted by every sync after, its body never pushed
        use crate::object::{ReplicaHash, ReplicaObject};

        let mutation = ReplicaMutation::Edit {
            sort_key: Default::default(),
            handle: ReplicaHandle::from("1"),
            object: ReplicaObject {
                hash: ReplicaHash::from("h-merged"),
                size: 6,
            },
            body: b"merged".to_vec(),
            meta: None,
        };
        let mut mutate = ReplicaMutate::new("inbox", mutation);
        let _ = mutate.resume(None);

        let mut loaded = loaded("1");
        loaded.placements[0].status = ReplicaStatus::Conflict;
        loaded.placements[0].object = Some(ReplicaHash::from("h-local"));
        loaded.placements[0].level = ReplicaLevel::Full;
        loaded.placements[0].conflict_revision = Some("r2".into());
        loaded.placements[0].conflict_object = Some(ReplicaHash::from("h-remote"));
        loaded.placements[0].base = None;

        let ops = match mutate.resume(Some(ReplicaArg::Load(loaded))) {
            ReplicaCoroutineState::Yielded(ReplicaYield::WantsWrite(ops)) => ops,
            state => panic!("expected WantsWrite, got {state:?}"),
        };
        let ReplicaWriteOp::UpsertPlacement(p) = &ops[1] else {
            panic!("expected UpsertPlacement, got {:?}", ops[1]);
        };
        let base = p.base.as_ref().expect("the resolution establishes a base");
        assert_eq!(base.revision.as_deref(), Some("r2"));
        assert_eq!(base.object, Some(ReplicaHash::from("h-remote")));
        assert_eq!(base.flags, p.flags, "nothing else is known of it");
    }

    #[test]
    fn edit_refreshes_the_projected_meta() {
        // a consumer projecting a fresh summary from the edited body
        // replaces the cached one
        use crate::object::{ReplicaHash, ReplicaObject};

        let mutation = ReplicaMutation::Edit {
            sort_key: Default::default(),
            handle: ReplicaHandle::from("1"),
            object: ReplicaObject {
                hash: ReplicaHash::from("h2"),
                size: 4,
            },
            body: b"body".to_vec(),
            meta: Some(ReplicaMeta("fresh".into())),
        };
        let mut mutate = ReplicaMutate::new("inbox", mutation);
        let _ = mutate.resume(None);

        let ops = match mutate.resume(Some(ReplicaArg::Load(loaded("1")))) {
            ReplicaCoroutineState::Yielded(ReplicaYield::WantsWrite(ops)) => ops,
            state => panic!("expected WantsWrite, got {state:?}"),
        };
        let ReplicaWriteOp::UpsertPlacement(p) = &ops[1] else {
            panic!("expected UpsertPlacement, got {:?}", ops[1]);
        };
        assert_eq!(p.meta, Some(ReplicaMeta("fresh".into())));
    }

    #[test]
    fn edit_resolves_a_conflict() {
        // the base adopts the remote revision observed at conflict time,
        // gating the resolving push on the state it was merged against,
        // and the recorded pair goes with it: the divergence the edit
        // settles has no reader left
        use crate::object::{ReplicaHash, ReplicaObject};

        let mutation = ReplicaMutation::Edit {
            sort_key: Default::default(),
            handle: ReplicaHandle::from("1"),
            object: ReplicaObject {
                hash: ReplicaHash::from("h3"),
                size: 6,
            },
            body: b"merged".to_vec(),
            meta: None,
        };
        let mut mutate = ReplicaMutate::new("inbox", mutation);
        let _ = mutate.resume(None);

        let mut loaded = loaded("1");
        loaded.placements[0].status = ReplicaStatus::Conflict;
        loaded.placements[0].conflict_revision = Some("r2".into());
        loaded.placements[0].conflict_object = Some(ReplicaHash::from("h-remote"));

        let ops = match mutate.resume(Some(ReplicaArg::Load(loaded))) {
            ReplicaCoroutineState::Yielded(ReplicaYield::WantsWrite(ops)) => ops,
            state => panic!("expected WantsWrite, got {state:?}"),
        };
        let ReplicaWriteOp::UpsertPlacement(p) = &ops[1] else {
            panic!("expected UpsertPlacement, got {:?}", ops[1]);
        };

        assert_eq!(p.status, ReplicaStatus::Dirty);
        assert_eq!(p.conflict_revision, None);
        assert_eq!(
            p.conflict_object, None,
            "the diverging body is dropped with the revision it named"
        );
        let base = p.base.as_ref().expect("a base");
        assert_eq!(base.revision.as_deref(), Some("r2"));
    }

    #[test]
    fn copy_stages_created_placement_in_target() {
        // the staged create carries the origin, so the push is a
        // server-side copy
        let mutation = ReplicaMutation::Copy {
            handle: ReplicaHandle::from("1"),
            target: "archive".into(),
            placeholder: ReplicaHandle::from("tmp-1"),
        };
        let mut mutate = ReplicaMutate::new("inbox", mutation);
        let _ = mutate.resume(None);

        let ops = match mutate.resume(Some(ReplicaArg::Load(loaded("1")))) {
            ReplicaCoroutineState::Yielded(ReplicaYield::WantsWrite(ops)) => ops,
            state => panic!("expected WantsWrite, got {state:?}"),
        };
        let ReplicaWriteOp::UpsertPlacement(p) = &ops[0] else {
            panic!("expected UpsertPlacement, got {:?}", ops[0]);
        };
        assert_eq!(p.collection.as_str(), "archive");
        assert_eq!(p.handle.as_str(), "tmp-1");
        assert_eq!(p.status, ReplicaStatus::Created);
        assert!(p.base.is_none());
        let origin = p.origin.as_ref().expect("the copy carries its origin");
        assert_eq!(origin.collection.as_str(), "inbox");
        assert_eq!(origin.handle.as_str(), "1");
    }

    #[test]
    fn move_stages_target_create_and_source_tombstone() {
        // the target's half copies and the source's half removes
        let mutation = ReplicaMutation::Move {
            handle: ReplicaHandle::from("1"),
            target: "archive".into(),
            placeholder: ReplicaHandle::from("tmp-1"),
        };
        let mut mutate = ReplicaMutate::new("inbox", mutation);
        let _ = mutate.resume(None);

        let ops = match mutate.resume(Some(ReplicaArg::Load(loaded("1")))) {
            ReplicaCoroutineState::Yielded(ReplicaYield::WantsWrite(ops)) => ops,
            state => panic!("expected WantsWrite, got {state:?}"),
        };

        let ReplicaWriteOp::UpsertPlacement(create) = &ops[0] else {
            panic!("expected UpsertPlacement, got {:?}", ops[0]);
        };
        assert_eq!(create.collection.as_str(), "archive");
        assert_eq!(create.handle.as_str(), "tmp-1");
        assert_eq!(create.status, ReplicaStatus::Created);
        assert!(create.base.is_none());
        assert_eq!(
            create
                .origin
                .as_ref()
                .expect("the move carries its origin")
                .handle
                .as_str(),
            "1",
        );

        let ReplicaWriteOp::UpsertPlacement(source) = &ops[1] else {
            panic!("expected UpsertPlacement, got {:?}", ops[1]);
        };
        assert_eq!(
            source.collection.as_str(),
            "inbox",
            "the source row, tombstoned"
        );
        assert_eq!(source.status, ReplicaStatus::Tombstone);
        assert_eq!(
            source
                .origin
                .as_ref()
                .expect("a move destination, so a source-first sync relocates rather than deletes")
                .collection
                .as_str(),
            "archive",
        );
    }

    #[test]
    fn a_mutation_reads_only_what_it_edits() {
        // the collection is not the unit of a local edit: a mutation
        // touches one row, and an Add only sees the rows that could
        // collide with its link id
        let mut mutate = ReplicaMutate::new("inbox", ReplicaMutation::Remove("7".into()));
        match mutate.resume(None) {
            ReplicaCoroutineState::Yielded(ReplicaYield::WantsLoad { scope, .. }) => {
                assert_eq!(
                    scope,
                    ReplicaLoadScope::Handles(vec![ReplicaHandle::from("7")])
                );
            }
            state => panic!("expected WantsLoad, got {state:?}"),
        }

        let add = ReplicaMutation::Add {
            handle: ReplicaHandle::from("tmp"),
            link_id: ReplicaLinkId::from("m1"),
            flags: ReplicaFlags::default(),
            object: ReplicaObject {
                hash: ReplicaHash::from("h"),
                size: 1,
            },
            body: vec![],
            meta: None,
            sort_key: Default::default(),
        };
        let mut mutate = ReplicaMutate::new("inbox", add);
        match mutate.resume(None) {
            ReplicaCoroutineState::Yielded(ReplicaYield::WantsLoad { scope, .. }) => {
                assert_eq!(
                    scope,
                    ReplicaLoadScope::Links(vec![ReplicaLinkId::from("m1")]),
                );
            }
            state => panic!("expected WantsLoad, got {state:?}"),
        }
    }

    #[test]
    fn a_move_of_an_unlinked_item_stages_the_relocation_alone() {
        // with no link id neither half can recognise what the other did,
        // so only the source-side relocation is staged
        let mut source = loaded("1");
        source.placements[0].link_id = None;

        let mutation = ReplicaMutation::Move {
            handle: ReplicaHandle::from("1"),
            target: "archive".into(),
            placeholder: ReplicaHandle::from("tmp-1"),
        };
        let mut mutate = ReplicaMutate::new("inbox", mutation);
        let _ = mutate.resume(None);

        let ops = match mutate.resume(Some(ReplicaArg::Load(source))) {
            ReplicaCoroutineState::Yielded(ReplicaYield::WantsWrite(ops)) => ops,
            state => panic!("expected WantsWrite, got {state:?}"),
        };
        assert_eq!(ops.len(), 1, "the tombstone alone, no target create");
        let ReplicaWriteOp::UpsertPlacement(source) = &ops[0] else {
            panic!("expected UpsertPlacement, got {:?}", ops[0]);
        };
        assert_eq!(source.collection.as_str(), "inbox");
        assert_eq!(source.status, ReplicaStatus::Tombstone);
    }
}
