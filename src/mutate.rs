//! I/O-free coroutine to mutate a collection locally, with no network.
//!
//! Loads the target placement, applies the change in memory, marks it
//! dirty or tombstone (the base is left untouched so the next sync can
//! derive the pending push), and writes it back. The remote is never
//! touched here; reconciliation is [`crate::sync`]'s job.

use alloc::{string::String, vec, vec::Vec};

use log::{debug, trace};
use thiserror::Error;

use crate::{
    change::ReplicaWriteOp,
    collection::ReplicaCollectionId,
    coroutine::*,
    object::ReplicaObject,
    placement::{
        ReplicaFlags, ReplicaHandle, ReplicaLevel, ReplicaLinkId, ReplicaMeta, ReplicaOrigin,
        ReplicaPlacement, ReplicaSortKey, ReplicaStatus,
    },
};

/// A local edit applied offline.
///
/// Each mutation reads one source placement in the coroutine's collection
/// and stages the resulting writes; the remote is reconciled on the next
/// sync. A copy stages a [`ReplicaStatus::Created`] placement in another
/// collection.
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
    /// object is stored, the placement repointed at it and marked dirty;
    /// the base keeps the previously synced body, so the next sync derives
    /// the pending push and a content three-way merge keeps its base
    /// bytes. Editing a conflicted placement resolves it: the remote
    /// revision observed at conflict time becomes the new base revision,
    /// so the resolving push is conditioned on the remote state the
    /// resolution was merged against.
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
        /// The refreshed sort key, on the same terms as `meta`: `None`
        /// keeps the stored one. An edit that changes what a key is
        /// derived from (a card's name, an event's start) has to say so,
        /// or the item stays where it was in the list.
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
    /// Move a placement into `target`: stage a `Created` placement in `target`
    /// under `placeholder` (carrying the source origin), and tombstone the
    /// source. The next sync derives a copy into the target and a remove from
    /// the source — the target create and the source tombstone land in their
    /// respective collection hubs.
    Move {
        /// The source placement to move.
        handle: ReplicaHandle,
        /// The collection to move it into.
        target: ReplicaCollectionId,
        /// The provisional handle the move is staged under in `target`.
        placeholder: ReplicaHandle,
    },
    /// Create a brand-new, locally-authored item with no remote origin (compose,
    /// import). Stages a pending create the next sync pushes as an append (the
    /// body is uploaded), not a server-side copy. Reads no existing source.
    Add {
        /// The provisional local handle the create is staged under, rekeyed to
        /// the server-assigned handle when the push reports it.
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
    /// The source handle the mutation reads in the coroutine's collection, or
    /// `None` for [`Add`](Self::Add), which creates a placement rather than
    /// reading one.
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
    /// The driver fed back an arg that does not match the pending yield.
    #[error("Replica MUTATE failed: unexpected coroutine arg")]
    UnexpectedArg,
    /// The driver resumed without the arg the pending yield required.
    #[error("Replica MUTATE failed: missing coroutine arg")]
    MissingArg,
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

    /// Stages the writes for the mutation given its loaded source placement.
    /// Flag sets and removes rewrite the source in place; a copy leaves the
    /// source untouched and stages a pending create in the target.
    fn writes(&self, mut source: ReplicaPlacement) -> Vec<ReplicaWriteOp> {
        match &self.mutation {
            ReplicaMutation::SetFlags { flags, .. } => {
                source.flags = flags.clone();
                // NOTE: a pending create stays a create and an unresolved
                // content conflict stays a conflict (its resolution is an
                // edit); the flag change rides along either way.
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

                // NOTE: editing a conflict is its resolution: the base
                // adopts the remote revision the resolution was merged
                // against.
                if source.status == ReplicaStatus::Conflict {
                    let revision = source.conflict_revision.take();
                    if let (Some(base), Some(revision)) = (source.base.as_mut(), revision) {
                        base.revision = Some(revision);
                    }
                }

                if source.status != ReplicaStatus::Created {
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
                let create = ReplicaPlacement {
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
                    base: None,
                    origin: Some(ReplicaOrigin {
                        collection: source.collection.clone(),
                        handle: source.handle.clone(),
                    }),
                };
                vec![ReplicaWriteOp::UpsertPlacement(create)]
            }
            ReplicaMutation::Move {
                target,
                placeholder,
                ..
            } => {
                // NOTE: stage a Created placement in the target (like Copy),
                // carrying the source origin, and tombstone the source. The
                // sync derives a copy into the target and a remove from the
                // source; a hub binding cannot carry a single atomic UID MOVE.
                let create = ReplicaPlacement {
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
                    base: None,
                    origin: Some(ReplicaOrigin {
                        collection: source.collection.clone(),
                        handle: source.handle.clone(),
                    }),
                };
                source.status = ReplicaStatus::Tombstone;
                source.origin = Some(ReplicaOrigin {
                    collection: target.clone(),
                    handle: source.handle.clone(),
                });
                vec![
                    ReplicaWriteOp::UpsertPlacement(create),
                    ReplicaWriteOp::UpsertPlacement(source),
                ]
            }
            // NOTE: Add reads no source; it is staged via `create_writes`.
            ReplicaMutation::Add { .. } => self.create_writes(),
        }
    }

    /// Stages the writes for an [`Add`](ReplicaMutation::Add): a locally-authored
    /// `Created` placement with no base and no origin (so the next sync appends
    /// it, uploading the body, rather than server-copying), plus its object.
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
                ReplicaCoroutineState::Yielded(ReplicaYield::WantsLoad(self.collection.clone()))
            }
            (State::PendingLoad, Some(ReplicaArg::Load(loaded))) => {
                let ops = if let ReplicaMutation::Add { link_id, .. } = &self.mutation {
                    // NOTE: no source to read; guard against re-creating a
                    // live item.
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
                ReplicaCoroutineState::Complete(Ok(()))
            }
            (_, Some(_)) => ReplicaCoroutineState::Complete(Err(ReplicaMutateError::UnexpectedArg)),
            (_, None) => ReplicaCoroutineState::Complete(Err(ReplicaMutateError::MissingArg)),
        }
    }
}

enum State {
    Start,
    PendingLoad,
    PendingWrite,
}

#[cfg(test)]
mod tests {
    use alloc::vec;

    use crate::{
        mutate::*,
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
                link_id: None,
                object: None,
                level: ReplicaLevel::Meta,
                meta: None,
                flags: ReplicaFlags::default(),
                conflict_revision: None,
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
        // The flag edit rides along; the content conflict stays unresolved
        // (its resolution is an edit), so the sync never mistakes the
        // placement for a plain dirty one.
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
        // A pending create keeps its status, else the sync would never
        // push the add.
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
        // A locally-authored item stages a Created placement with no base and
        // no origin, plus its body — the shape the sync pushes as an append
        // (uploading the body), not a server-side copy.
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

        // Add reads no source, but still loads to guard collisions; the loaded
        // item is unrelated (no link id), so no collision.
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

        // A live placement already holds mid:dup.
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
        // A tombstoned item with the same link id does not block a re-create
        // (the delete is in flight; the new item supersedes it).
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
            ReplicaCoroutineState::Complete(Err(ReplicaMutateError::MissingArg)) => {}
            state => panic!("expected MissingArg, got {state:?}"),
        }
    }

    #[test]
    fn unexpected_arg_errors() {
        let mutation = ReplicaMutation::Remove(ReplicaHandle::from("1"));
        let mut mutate = ReplicaMutate::new("inbox", mutation);
        let _ = mutate.resume(None);

        match mutate.resume(Some(ReplicaArg::Write)) {
            ReplicaCoroutineState::Complete(Err(ReplicaMutateError::UnexpectedArg)) => {}
            state => panic!("expected UnexpectedArg, got {state:?}"),
        }
    }

    #[test]
    fn edit_stages_a_dirty_body() {
        // An edit stores the new object, repoints the placement at it at
        // full level and marks it dirty; the base keeps the synced state so
        // the next sync derives the push.
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
    fn edit_refreshes_the_projected_meta() {
        // A consumer that projects a fresh summary from the edited body
        // passes it along; the cached one is replaced.
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
        // Editing a conflicted placement is its resolution: the base adopts
        // the remote revision observed at conflict time, so the resolving
        // push is gated on the remote state the merge was made against.
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

        let ops = match mutate.resume(Some(ReplicaArg::Load(loaded))) {
            ReplicaCoroutineState::Yielded(ReplicaYield::WantsWrite(ops)) => ops,
            state => panic!("expected WantsWrite, got {state:?}"),
        };
        let ReplicaWriteOp::UpsertPlacement(p) = &ops[1] else {
            panic!("expected UpsertPlacement, got {:?}", ops[1]);
        };

        assert_eq!(p.status, ReplicaStatus::Dirty);
        assert_eq!(p.conflict_revision, None);
        let base = p.base.as_ref().expect("a base");
        assert_eq!(base.revision.as_deref(), Some("r2"));
    }

    #[test]
    fn copy_stages_created_placement_in_target() {
        // A copy leaves the source untouched and stages a Created placement
        // in the target under the placeholder, carrying its origin.
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
        // A move stages a Created placement in the target (carrying the source
        // origin) and tombstones the source, so the sync copies into the target
        // and removes from the source.
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
                .expect("a move target")
                .collection
                .as_str(),
            "archive",
        );
    }
}
