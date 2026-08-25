//! I/O-free coroutine to rebuild a collection after a handle-space
//! change, carrying local state over by link id.
//!
//! A protocol may invalidate every handle at once (an IMAP UIDVALIDITY
//! bump renumbers all UIDs). A plain full sync recovers the spine but
//! reads every old handle as deleted upstream, dropping cached bodies
//! and pending local changes with them. This verb enumerates the new
//! spine, resolves the new link ids at the meta tier, and carries every
//! old placement over to the new handle of the same logical item: the
//! cache (level, summary, body) survives without a refetch, flag deltas
//! re-derive against the new base, tombstones keep their pending remove
//! (and move destination), and staged edits keep their body. A staged
//! edit whose item found no new home survives as a pending create (the
//! same edit-beats-delete rule the sync applies); any other pending
//! state that cannot be matched is dropped and counted. Pending creates
//! are local staging, not spine, and are left untouched.
//!
//! The carried base adopts the new observed revision, so a carried edit
//! pushes last-writer-wins on its first sync: the old revision chain is
//! gone with the old handles, and there is no base revision to gate on.

use core::mem;

use alloc::{
    collections::{BTreeMap, BTreeSet},
    vec::Vec,
};

use log::{debug, trace};
use thiserror::Error;

use crate::{
    change::{ReplicaDropReason, ReplicaWriteOp},
    collection::{ReplicaCheckpoint, ReplicaCollectionId},
    coroutine::*,
    placement::{
        ReplicaBase, ReplicaFlags, ReplicaHandle, ReplicaLevel, ReplicaLinkId, ReplicaMeta,
        ReplicaPlacement, ReplicaSortKey, ReplicaStatus,
    },
    remote::{ReplicaRemoteItem, ReplicaTier},
    storage::ReplicaLoadScope,
};

/// What a rekey did.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ReplicaRekeyReport {
    /// Old placements carried over to their new handle.
    pub rekeyed: usize,
    /// New members with no old placement to carry, pulled fresh.
    pub pulled: usize,
    /// Old placements with pending local state that could not be matched
    /// (no link id resolved before the handle-space change, or the item
    /// is gone from the new spine) and were dropped with it.
    pub dropped: usize,
}

/// Failure causes during a REKEY flow.
#[derive(Clone, Debug, Error)]
pub enum ReplicaRekeyError {
    /// The driver fed back an arg that does not match the pending yield.
    #[error("Replica REKEY failed: unexpected coroutine arg")]
    UnexpectedArg,
    /// The driver resumed without the arg the pending yield required.
    #[error("Replica REKEY failed: missing coroutine arg")]
    MissingArg,
}

/// I/O-free REKEY coroutine.
pub struct ReplicaRekey {
    collection: ReplicaCollectionId,
    old: Vec<ReplicaPlacement>,
    items: Vec<ReplicaRemoteItem>,
    checkpoint: Option<ReplicaCheckpoint>,
    report: ReplicaRekeyReport,
    state: State,
}

impl ReplicaRekey {
    /// Creates a coroutine that rebuilds `collection` onto its new
    /// handle space.
    pub fn new(collection: impl Into<ReplicaCollectionId>) -> Self {
        let collection = collection.into();
        debug!("rekey collection {}", collection.as_str());

        Self {
            collection,
            old: Vec::new(),
            items: Vec::new(),
            checkpoint: None,
            report: ReplicaRekeyReport::default(),
            state: State::Start,
        }
    }

    /// Builds the write batch: drop the old spine, then upsert one
    /// placement per new member, carried over by link id when an old
    /// placement resolves to the same logical item.
    fn rebuild(
        &mut self,
        links: BTreeMap<ReplicaHandle, (ReplicaLinkId, ReplicaMeta, ReplicaSortKey)>,
    ) -> Vec<ReplicaWriteOp> {
        let mut writes = Vec::new();

        // NOTE: pending creates are local staging, not spine: left untouched
        let old: Vec<ReplicaPlacement> = mem::take(&mut self.old)
            .into_iter()
            .filter(|p| p.status != ReplicaStatus::Created)
            .collect();

        let mut old_by_link: BTreeMap<ReplicaLinkId, ReplicaPlacement> = old
            .iter()
            .filter_map(|p| Some((p.link_id.clone()?, p.clone())))
            .collect();

        // NOTE: the whole old spine goes, but nothing here is a delete:
        // every row is either about to be rewritten under its new handle
        // or lost with a handle space the server has already discarded.
        // Marking the drops superseded is what keeps a storage sharing one
        // item across sources from reading a renumbering as a mass delete,
        // and what makes the batch order-insensitive.
        for placement in &old {
            writes.push(ReplicaWriteOp::DropPlacement {
                collection: self.collection.clone(),
                handle: placement.handle.clone(),
                reason: ReplicaDropReason::Superseded,
            });
        }

        let mut carried_over = BTreeSet::new();
        for item in mem::take(&mut self.items) {
            let resolved = links.get(&item.handle);
            let carried = resolved.and_then(|(link, _, _)| old_by_link.remove(link));

            match carried {
                Some(old) => {
                    carried_over.insert(old.handle.clone());
                    writes.push(ReplicaWriteOp::UpsertPlacement(
                        self.carry(old, &item, resolved),
                    ));
                    self.report.rekeyed += 1;
                }
                None => {
                    writes.push(ReplicaWriteOp::UpsertPlacement(self.fresh(&item, resolved)));
                    self.report.pulled += 1;
                }
            }
        }

        // NOTE: an unmatched staged edit survives as a pending create, the
        // same edit-beats-delete rule the sync applies when a remote delete
        // races a local edit; every other unmatched pending state is lost
        // with the old handle space
        for placement in &old {
            if carried_over.contains(&placement.handle) {
                continue;
            }
            let edited = matches!(
                placement.status,
                ReplicaStatus::Dirty | ReplicaStatus::Conflict
            ) && placement.object.is_some()
                && placement
                    .base
                    .as_ref()
                    .is_none_or(|b| b.object != placement.object);
            if edited {
                let mut resurrected = placement.clone();
                resurrected.status = ReplicaStatus::Created;
                resurrected.conflict_revision = None;
                resurrected.base = None;
                resurrected.origin = None;
                writes.push(ReplicaWriteOp::UpsertPlacement(resurrected));
                carried_over.insert(placement.handle.clone());
                self.report.rekeyed += 1;
            }
        }
        self.report.dropped += old
            .iter()
            .filter(|p| p.status != ReplicaStatus::Clean && !carried_over.contains(&p.handle))
            .count();

        writes.push(ReplicaWriteOp::SetCheckpoint {
            collection: self.collection.clone(),
            checkpoint: self.checkpoint.take().expect("an enumerated checkpoint"),
        });

        writes
    }

    /// Carries an old placement onto the new handle: the cache survives,
    /// the flag delta re-derives against the new base, pending statuses
    /// stay pending.
    fn carry(
        &self,
        old: ReplicaPlacement,
        item: &ReplicaRemoteItem,
        resolved: Option<&(ReplicaLinkId, ReplicaMeta, ReplicaSortKey)>,
    ) -> ReplicaPlacement {
        let old_base_flags = old
            .base
            .as_ref()
            .map(|b| b.flags.clone())
            .unwrap_or_default();
        let flags = ReplicaFlags::merge(&old_base_flags, &old.flags, &item.flags);

        let content_edit = old.status == ReplicaStatus::Dirty
            && old.object.is_some()
            && old.base.as_ref().is_none_or(|b| b.object != old.object);
        let status = match old.status {
            ReplicaStatus::Tombstone => ReplicaStatus::Tombstone,
            ReplicaStatus::Conflict => ReplicaStatus::Conflict,
            // NOTE: a handle-space change renumbers the copies, it does not
            // merge them: the source still holds the identity twice, so the
            // freeze carries over. The recorded handles belong to the old
            // space, and the next complete enumeration clears them, after
            // which the meta fetch re-detects the duplicate under its new
            // handles if it is still there.
            ReplicaStatus::Ambiguous => ReplicaStatus::Ambiguous,
            _ if content_edit => ReplicaStatus::Dirty,
            _ if flags != item.flags => ReplicaStatus::Dirty,
            _ => ReplicaStatus::Clean,
        };
        let conflict_revision = if status == ReplicaStatus::Conflict {
            item.revision.clone()
        } else {
            None
        };

        ReplicaPlacement {
            collection: self.collection.clone(),
            handle: item.handle.clone(),
            link_id: old.link_id.clone(),
            object: old.object.clone(),
            level: old.level,
            sort_key: resolved
                .map(|(_, _, key)| key.clone())
                .unwrap_or_else(|| old.sort_key.clone()),
            meta: resolved
                .map(|(_, meta, _)| meta.clone())
                .or_else(|| old.meta.clone()),
            flags,
            status,
            conflict_revision,
            base: Some(ReplicaBase {
                flags: item.flags.clone(),
                revision: item.revision.clone(),
                object: old.base.as_ref().and_then(|b| b.object.clone()),
            }),
            origin: old.origin,
            ambiguous_handles: old.ambiguous_handles,
        }
    }

    /// A fresh placement for a new member with no old counterpart,
    /// enriched with the link id and summary the meta fetch resolved.
    fn fresh(
        &self,
        item: &ReplicaRemoteItem,
        resolved: Option<&(ReplicaLinkId, ReplicaMeta, ReplicaSortKey)>,
    ) -> ReplicaPlacement {
        ReplicaPlacement {
            collection: self.collection.clone(),
            handle: item.handle.clone(),
            link_id: resolved.map(|(link, _, _)| link.clone()),
            object: None,
            level: if resolved.is_some() {
                ReplicaLevel::Meta
            } else {
                ReplicaLevel::Probed
            },
            meta: resolved.map(|(_, meta, _)| meta.clone()),
            sort_key: resolved.map(|(_, _, key)| key.clone()).unwrap_or_default(),
            flags: item.flags.clone(),
            status: ReplicaStatus::Clean,
            conflict_revision: None,
            base: Some(ReplicaBase {
                flags: item.flags.clone(),
                revision: item.revision.clone(),
                object: None,
            }),
            origin: None,
            ambiguous_handles: Vec::new(),
        }
    }
}

impl ReplicaCoroutine for ReplicaRekey {
    type Yield = ReplicaYield;
    type Return = Result<ReplicaRekeyReport, ReplicaRekeyError>;

    fn resume(
        &mut self,
        arg: Option<ReplicaArg>,
    ) -> ReplicaCoroutineState<Self::Yield, Self::Return> {
        match (&self.state, arg) {
            (State::Start, None) => {
                debug!("load local state from storage");
                self.state = State::PendingLoad;
                ReplicaCoroutineState::Yielded(ReplicaYield::WantsLoad {
                    collection: self.collection.clone(),
                    scope: ReplicaLoadScope::All,
                })
            }

            (State::PendingLoad, Some(ReplicaArg::Load(loaded))) => {
                self.old = loaded.placements;

                debug!("enumerate the new handle space in full");
                trace!("loaded {} old placements", self.old.len());
                self.state = State::PendingEnumerate;
                ReplicaCoroutineState::Yielded(ReplicaYield::WantsEnumerate {
                    collection: self.collection.clone(),
                    cursor: None,
                })
            }

            (State::PendingEnumerate, Some(ReplicaArg::Enumerate(snapshot))) => {
                self.items = snapshot.items;
                self.checkpoint = Some(snapshot.checkpoint);

                // NOTE: without a single resolved link id there is nothing
                // to match against: rebuild the spine without a fetch
                if !self.old.iter().any(|p| p.link_id.is_some()) {
                    debug!("no link ids to match, rebuild the spine");
                    self.state = State::PendingWrite;
                    let writes = self.rebuild(BTreeMap::new());
                    return ReplicaCoroutineState::Yielded(ReplicaYield::WantsWrite(writes));
                }

                let handles: Vec<ReplicaHandle> =
                    self.items.iter().map(|i| i.handle.clone()).collect();
                debug!("resolve {} new link ids at meta tier", handles.len());
                self.state = State::PendingFetch;
                ReplicaCoroutineState::Yielded(ReplicaYield::WantsFetch {
                    collection: self.collection.clone(),
                    handles,
                    tier: ReplicaTier::Meta,
                })
            }

            (State::PendingFetch, Some(ReplicaArg::Fetch(fetched))) => {
                let links: BTreeMap<ReplicaHandle, (ReplicaLinkId, ReplicaMeta, ReplicaSortKey)> =
                    fetched
                        .into_iter()
                        .map(|f| (f.handle, (f.link_id, f.meta, f.sort_key)))
                        .collect();

                trace!("resolved {} link ids", links.len());
                self.state = State::PendingWrite;
                let writes = self.rebuild(links);
                ReplicaCoroutineState::Yielded(ReplicaYield::WantsWrite(writes))
            }

            (State::PendingWrite, Some(ReplicaArg::Write)) => {
                debug!(
                    "rekey done: {} carried, {} pulled, {} pending dropped",
                    self.report.rekeyed, self.report.pulled, self.report.dropped,
                );
                ReplicaCoroutineState::Complete(Ok(self.report))
            }

            (_, Some(_)) => ReplicaCoroutineState::Complete(Err(ReplicaRekeyError::UnexpectedArg)),
            (_, None) => ReplicaCoroutineState::Complete(Err(ReplicaRekeyError::MissingArg)),
        }
    }
}

enum State {
    Start,
    PendingLoad,
    PendingEnumerate,
    PendingFetch,
    PendingWrite,
}

#[cfg(test)]
mod tests {
    use alloc::vec;

    use crate::{
        object::ReplicaHash,
        placement::ReplicaOrigin,
        rekey::*,
        remote::{ReplicaFetchedItem, ReplicaRemoteSnapshot},
        storage::ReplicaLoaded,
    };

    /// An old-spine placement, synced clean at base `flags`.
    fn synced(handle: &str, link: &str, flags: &[&str]) -> ReplicaPlacement {
        ReplicaPlacement {
            sort_key: Default::default(),
            collection: "inbox".into(),
            handle: ReplicaHandle::from(handle),
            link_id: Some(ReplicaLinkId::from(link)),
            object: None,
            level: ReplicaLevel::Meta,
            meta: Some(ReplicaMeta("row".into())),
            flags: ReplicaFlags::from_iter(flags.iter().copied()),
            status: ReplicaStatus::Clean,
            conflict_revision: None,
            base: Some(ReplicaBase {
                flags: ReplicaFlags::from_iter(flags.iter().copied()),
                revision: None,
                object: None,
            }),
            origin: None,
            ambiguous_handles: Vec::new(),
        }
    }

    fn item(handle: &str, flags: &[&str]) -> ReplicaRemoteItem {
        ReplicaRemoteItem {
            handle: ReplicaHandle::from(handle),
            flags: ReplicaFlags::from_iter(flags.iter().copied()),
            revision: None,
        }
    }

    fn fetched(handle: &str, link: &str) -> ReplicaFetchedItem {
        ReplicaFetchedItem {
            sort_key: Default::default(),
            handle: ReplicaHandle::from(handle),
            link_id: ReplicaLinkId::from(link),
            meta: ReplicaMeta("fresh row".into()),
            body: None,
            revision: None,
        }
    }

    /// Runs a rekey to completion over the given old spine, new spine and
    /// meta replies, returning the writes and the report.
    fn run(
        old: Vec<ReplicaPlacement>,
        items: Vec<ReplicaRemoteItem>,
        metas: Vec<ReplicaFetchedItem>,
    ) -> (Vec<ReplicaWriteOp>, ReplicaRekeyReport) {
        crate::testlog::init();
        let mut rekey = ReplicaRekey::new("inbox");
        let _ = rekey.resume(None);
        let _ = rekey.resume(Some(ReplicaArg::Load(ReplicaLoaded {
            placements: old,
            checkpoint: None,
        })));

        let snapshot = ReplicaRemoteSnapshot {
            items,
            vanished: Vec::new(),
            complete: true,
            checkpoint: ReplicaCheckpoint(b"v2".to_vec()),
        };
        let writes = match rekey.resume(Some(ReplicaArg::Enumerate(snapshot))) {
            ReplicaCoroutineState::Yielded(ReplicaYield::WantsFetch { tier, .. }) => {
                assert_eq!(tier, ReplicaTier::Meta);
                match rekey.resume(Some(ReplicaArg::Fetch(metas))) {
                    ReplicaCoroutineState::Yielded(ReplicaYield::WantsWrite(w)) => w,
                    state => panic!("expected WantsWrite, got {state:?}"),
                }
            }
            ReplicaCoroutineState::Yielded(ReplicaYield::WantsWrite(w)) => w,
            state => panic!("expected fetch or write, got {state:?}"),
        };

        let report = match rekey.resume(Some(ReplicaArg::Write)) {
            ReplicaCoroutineState::Complete(Ok(report)) => report,
            state => panic!("expected Complete(Ok), got {state:?}"),
        };
        (writes, report)
    }

    fn upserted<'a>(writes: &'a [ReplicaWriteOp], handle: &str) -> Option<&'a ReplicaPlacement> {
        writes.iter().find_map(|w| match w {
            ReplicaWriteOp::UpsertPlacement(p) if p.handle.as_str() == handle => Some(p),
            _ => None,
        })
    }

    #[test]
    fn a_pending_flag_delta_survives_the_bump() {
        // The old placement had staged "flagged" on top of a "seen" base;
        // the new handle re-derives that delta against the new base.
        let mut old = synced("1", "msg-a", &["seen"]);
        old.flags = ReplicaFlags::from_iter(["seen", "flagged"]);
        old.status = ReplicaStatus::Dirty;

        let (writes, report) = run(
            vec![old],
            vec![item("101", &["seen"])],
            vec![fetched("101", "msg-a")],
        );

        assert_eq!(report.rekeyed, 1);
        assert_eq!(report.dropped, 0);
        assert!(
            writes.iter().any(
                |w| matches!(w, ReplicaWriteOp::DropPlacement { handle, .. } if handle.as_str() == "1")
            ),
            "the old handle is dropped: {writes:?}",
        );
        let carried = upserted(&writes, "101").expect("a carried placement");
        assert_eq!(
            carried.status,
            ReplicaStatus::Dirty,
            "the delta stays pending"
        );
        assert!(carried.flags.contains("flagged"));
        let base = carried.base.as_ref().expect("a base");
        assert!(base.flags.contains("seen") && !base.flags.contains("flagged"));
    }

    #[test]
    fn a_tombstone_survives_with_its_destination() {
        let mut old = synced("1", "msg-a", &["seen"]);
        old.status = ReplicaStatus::Tombstone;
        old.origin = Some(ReplicaOrigin {
            collection: "archive".into(),
            handle: ReplicaHandle::from("1"),
        });

        let (writes, report) = run(
            vec![old],
            vec![item("101", &["seen"])],
            vec![fetched("101", "msg-a")],
        );

        assert_eq!(report.rekeyed, 1);
        let carried = upserted(&writes, "101").expect("a carried placement");
        assert_eq!(carried.status, ReplicaStatus::Tombstone);
        assert_eq!(
            carried.origin.as_ref().expect("a move target").collection,
            "archive".into(),
        );
    }

    #[test]
    fn a_staged_edit_survives_with_its_body() {
        let mut old = synced("1", "msg-a", &[]);
        old.object = Some(ReplicaHash::from("h2"));
        old.level = ReplicaLevel::Full;
        old.status = ReplicaStatus::Dirty;

        let (writes, report) = run(
            vec![old],
            vec![item("101", &[])],
            vec![fetched("101", "msg-a")],
        );

        assert_eq!(report.rekeyed, 1);
        let carried = upserted(&writes, "101").expect("a carried placement");
        assert_eq!(carried.status, ReplicaStatus::Dirty);
        assert_eq!(
            carried.object,
            Some(ReplicaHash::from("h2")),
            "the body survives"
        );
        assert_eq!(carried.level, ReplicaLevel::Full, "the cache survives");
    }

    #[test]
    fn a_clean_cache_carries_over_without_pending_state() {
        let mut old = synced("1", "msg-a", &["seen"]);
        old.object = Some(ReplicaHash::from("h1"));
        old.base.as_mut().expect("a base").object = Some(ReplicaHash::from("h1"));
        old.level = ReplicaLevel::Full;

        let (writes, report) = run(
            vec![old],
            vec![item("101", &["seen"])],
            vec![fetched("101", "msg-a")],
        );

        assert_eq!(report.rekeyed, 1);
        let carried = upserted(&writes, "101").expect("a carried placement");
        assert_eq!(carried.status, ReplicaStatus::Clean);
        assert_eq!(carried.object, Some(ReplicaHash::from("h1")));
        assert_eq!(carried.level, ReplicaLevel::Full);
        let base = carried.base.as_ref().expect("a base");
        assert_eq!(base.object, Some(ReplicaHash::from("h1")));
    }

    #[test]
    fn an_unmatched_staged_edit_resurrects_as_a_pending_create() {
        // The edited item is gone from the new spine (deleted during the
        // outage that came with the bump): the edit survives as a pending
        // create, the same edit-beats-delete rule the sync applies.
        let mut old = synced("1", "msg-a", &[]);
        old.object = Some(ReplicaHash::from("h2"));
        old.level = ReplicaLevel::Full;
        old.status = ReplicaStatus::Dirty;

        let (writes, report) = run(vec![old], vec![], vec![]);

        assert_eq!(report.rekeyed, 1, "carried as a pending create");
        assert_eq!(report.dropped, 0);
        let resurrected = upserted(&writes, "1").expect("a resurrected placement");
        assert_eq!(resurrected.status, ReplicaStatus::Created);
        assert!(resurrected.base.is_none());
        assert_eq!(resurrected.object, Some(ReplicaHash::from("h2")));
    }

    #[test]
    fn unmatched_pending_state_is_dropped_and_counted() {
        // A probed-only placement (no link id resolved) cannot be matched:
        // its pending flag edit is lost with the old handle space.
        let mut old = synced("1", "msg-a", &[]);
        old.link_id = None;
        old.flags = ReplicaFlags::from_iter(["flagged"]);
        old.status = ReplicaStatus::Dirty;

        let (writes, report) = run(vec![old], vec![item("101", &[])], vec![]);

        assert_eq!(report.rekeyed, 0);
        assert_eq!(report.pulled, 1);
        assert_eq!(report.dropped, 1, "the pending edit is lost, and said so");
        let fresh = upserted(&writes, "101").expect("a fresh placement");
        assert_eq!(fresh.status, ReplicaStatus::Clean);
    }

    #[test]
    fn no_link_ids_skips_the_meta_fetch() {
        let mut old = synced("1", "msg-a", &[]);
        old.link_id = None;

        let mut rekey = ReplicaRekey::new("inbox");
        let _ = rekey.resume(None);
        let _ = rekey.resume(Some(ReplicaArg::Load(ReplicaLoaded {
            placements: vec![old],
            checkpoint: None,
        })));

        let snapshot = ReplicaRemoteSnapshot {
            items: vec![item("101", &[])],
            vanished: Vec::new(),
            complete: true,
            checkpoint: ReplicaCheckpoint(b"v2".to_vec()),
        };
        match rekey.resume(Some(ReplicaArg::Enumerate(snapshot))) {
            ReplicaCoroutineState::Yielded(ReplicaYield::WantsWrite(_)) => {}
            state => panic!("expected WantsWrite without a fetch, got {state:?}"),
        }
    }

    #[test]
    fn pending_creates_are_left_untouched() {
        let mut placeholder = synced("tmp-1", "msg-b", &[]);
        placeholder.status = ReplicaStatus::Created;
        placeholder.base = None;

        let (writes, report) = run(vec![placeholder], vec![], vec![]);

        assert_eq!(report.rekeyed + report.pulled + report.dropped, 0);
        assert!(
            !writes.iter().any(|w| matches!(
                w,
                ReplicaWriteOp::DropPlacement { handle, .. } if handle.as_str() == "tmp-1"
            )),
            "the placeholder is not spine, it stays: {writes:?}",
        );
    }

    #[test]
    fn missing_arg_errors() {
        let mut rekey = ReplicaRekey::new("inbox");
        let _ = rekey.resume(None);
        match rekey.resume(None) {
            ReplicaCoroutineState::Complete(Err(ReplicaRekeyError::MissingArg)) => {}
            state => panic!("expected MissingArg, got {state:?}"),
        }
    }

    #[test]
    fn unexpected_arg_errors() {
        let mut rekey = ReplicaRekey::new("inbox");
        let _ = rekey.resume(None);
        match rekey.resume(Some(ReplicaArg::Write)) {
            ReplicaCoroutineState::Complete(Err(ReplicaRekeyError::UnexpectedArg)) => {}
            state => panic!("expected UnexpectedArg, got {state:?}"),
        }
    }

    #[test]
    fn an_ambiguous_identity_survives_a_handle_space_change() {
        // Renumbering the copies does not merge them: the source still holds
        // the identity twice, so the freeze carries over rather than the item
        // becoming deletable again on the other side of a UIDVALIDITY bump.
        let mut old = synced("7", "m1", &[]);
        old.status = ReplicaStatus::Ambiguous;
        old.ambiguous_handles = vec![ReplicaHandle::from("8")];

        let (writes, report) = run(
            vec![old],
            vec![item("v2-0", &[])],
            vec![fetched("v2-0", "m1")],
        );

        assert_eq!(report.rekeyed, 1);
        let carried = writes
            .iter()
            .find_map(|op| match op {
                ReplicaWriteOp::UpsertPlacement(p) if p.handle.as_str() == "v2-0" => Some(p),
                _ => None,
            })
            .expect("the carried placement");
        assert_eq!(carried.status, ReplicaStatus::Ambiguous);
        assert_eq!(carried.ambiguous_handles, vec![ReplicaHandle::from("8")]);
    }
}
