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

use core::fmt;

use alloc::{
    collections::{BTreeMap, BTreeSet},
    vec::Vec,
};

use log::{debug, trace};
use thiserror::Error;

use crate::{
    change::WriteOp,
    collection::{Checkpoint, CollectionId},
    coroutine::*,
    placement::{Base, Flags, Handle, Level, LinkId, Meta, Placement, Status},
    remote::{RemoteItem, Tier},
};

/// What a rekey did.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct OfflineRekeyReport {
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
pub enum OfflineRekeyError {
    /// The driver fed back an arg that does not match the pending yield.
    #[error("Offline REKEY failed: unexpected coroutine arg")]
    UnexpectedArg,
    /// The driver resumed without the arg the pending yield required.
    #[error("Offline REKEY failed: missing coroutine arg")]
    MissingArg,
}

/// I/O-free REKEY coroutine.
pub struct OfflineRekey {
    collection: CollectionId,
    old: Vec<Placement>,
    items: Vec<RemoteItem>,
    checkpoint: Option<Checkpoint>,
    report: OfflineRekeyReport,
    state: State,
}

impl OfflineRekey {
    /// Creates a coroutine that rebuilds `collection` onto its new
    /// handle space.
    pub fn new(collection: impl Into<CollectionId>) -> Self {
        let collection = collection.into();
        debug!("rekey collection {}", collection.as_str());

        Self {
            collection,
            old: Vec::new(),
            items: Vec::new(),
            checkpoint: None,
            report: OfflineRekeyReport::default(),
            state: State::Start,
        }
    }

    /// Builds the write batch: drop the old spine, then upsert one
    /// placement per new member, carried over by link id when an old
    /// placement resolves to the same logical item.
    fn rebuild(&mut self, links: BTreeMap<Handle, (LinkId, Meta)>) -> Vec<WriteOp> {
        let mut writes = Vec::new();

        // pending creates are local staging, not spine: left untouched
        let old: Vec<Placement> = core::mem::take(&mut self.old)
            .into_iter()
            .filter(|p| p.status != Status::Created)
            .collect();

        let mut old_by_link: BTreeMap<LinkId, Placement> = old
            .iter()
            .filter_map(|p| Some((p.link_id.clone()?, p.clone())))
            .collect();

        for placement in &old {
            writes.push(WriteOp::DropPlacement {
                collection: self.collection.clone(),
                handle: placement.handle.clone(),
            });
        }

        let mut carried_over = BTreeSet::new();
        for item in core::mem::take(&mut self.items) {
            let resolved = links.get(&item.handle);
            let carried = resolved.and_then(|(link, _)| old_by_link.remove(link));

            match carried {
                Some(old) => {
                    carried_over.insert(old.handle.clone());
                    writes.push(WriteOp::UpsertPlacement(self.carry(old, &item, resolved)));
                    self.report.rekeyed += 1;
                }
                None => {
                    writes.push(WriteOp::UpsertPlacement(self.fresh(&item, resolved)));
                    self.report.pulled += 1;
                }
            }
        }

        // an unmatched staged edit survives as a pending create, the same
        // edit-beats-delete rule the sync applies when a remote delete
        // races a local edit; every other unmatched pending state is lost
        // with the old handle space
        for placement in &old {
            if carried_over.contains(&placement.handle) {
                continue;
            }
            let edited = matches!(placement.status, Status::Dirty | Status::Conflict)
                && placement.object.is_some()
                && placement
                    .base
                    .as_ref()
                    .is_none_or(|b| b.object != placement.object);
            if edited {
                let mut resurrected = placement.clone();
                resurrected.status = Status::Created;
                resurrected.conflict_revision = None;
                resurrected.base = None;
                resurrected.origin = None;
                writes.push(WriteOp::UpsertPlacement(resurrected));
                carried_over.insert(placement.handle.clone());
                self.report.rekeyed += 1;
            }
        }
        self.report.dropped += old
            .iter()
            .filter(|p| p.status != Status::Clean && !carried_over.contains(&p.handle))
            .count();

        writes.push(WriteOp::SetCheckpoint {
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
        old: Placement,
        item: &RemoteItem,
        resolved: Option<&(LinkId, Meta)>,
    ) -> Placement {
        let old_base_flags = old
            .base
            .as_ref()
            .map(|b| b.flags.clone())
            .unwrap_or_default();
        let flags = Flags::merge(&old_base_flags, &old.flags, &item.flags);

        let content_edit = old.status == Status::Dirty
            && old.object.is_some()
            && old.base.as_ref().is_none_or(|b| b.object != old.object);
        let status = match old.status {
            Status::Tombstone => Status::Tombstone,
            Status::Conflict => Status::Conflict,
            _ if content_edit => Status::Dirty,
            _ if flags != item.flags => Status::Dirty,
            _ => Status::Clean,
        };
        let conflict_revision = if status == Status::Conflict {
            item.revision.clone()
        } else {
            None
        };

        Placement {
            collection: self.collection.clone(),
            handle: item.handle.clone(),
            link_id: old.link_id.clone(),
            object: old.object.clone(),
            level: old.level,
            meta: resolved
                .map(|(_, meta)| meta.clone())
                .or_else(|| old.meta.clone()),
            flags,
            status,
            conflict_revision,
            base: Some(Base {
                flags: item.flags.clone(),
                revision: item.revision.clone(),
                object: old.base.as_ref().and_then(|b| b.object.clone()),
            }),
            origin: old.origin,
        }
    }

    /// A fresh placement for a new member with no old counterpart,
    /// enriched with the link id and summary the meta fetch resolved.
    fn fresh(&self, item: &RemoteItem, resolved: Option<&(LinkId, Meta)>) -> Placement {
        Placement {
            collection: self.collection.clone(),
            handle: item.handle.clone(),
            link_id: resolved.map(|(link, _)| link.clone()),
            object: None,
            level: if resolved.is_some() {
                Level::Meta
            } else {
                Level::Probed
            },
            meta: resolved.map(|(_, meta)| meta.clone()),
            flags: item.flags.clone(),
            status: Status::Clean,
            conflict_revision: None,
            base: Some(Base {
                flags: item.flags.clone(),
                revision: item.revision.clone(),
                object: None,
            }),
            origin: None,
        }
    }
}

impl OfflineCoroutine for OfflineRekey {
    type Yield = OfflineYield;
    type Return = Result<OfflineRekeyReport, OfflineRekeyError>;

    fn resume(
        &mut self,
        arg: Option<OfflineArg>,
    ) -> OfflineCoroutineState<Self::Yield, Self::Return> {
        trace!("rekey: {}", self.state);

        match (&self.state, arg) {
            (State::Start, None) => {
                debug!("load local state from storage");
                self.state = State::PendingLoad;
                OfflineCoroutineState::Yielded(OfflineYield::WantsLoad(self.collection.clone()))
            }

            (State::PendingLoad, Some(OfflineArg::Load(loaded))) => {
                self.old = loaded.placements;

                debug!("enumerate the new handle space in full");
                trace!("loaded {} old placements", self.old.len());
                self.state = State::PendingEnumerate;
                OfflineCoroutineState::Yielded(OfflineYield::WantsEnumerate {
                    collection: self.collection.clone(),
                    cursor: None,
                })
            }

            (State::PendingEnumerate, Some(OfflineArg::Enumerate(snapshot))) => {
                self.items = snapshot.items;
                self.checkpoint = Some(snapshot.checkpoint);

                // without a single resolved link id there is nothing to
                // match against: rebuild the spine without a fetch
                if !self.old.iter().any(|p| p.link_id.is_some()) {
                    debug!("no link ids to match, rebuild the spine");
                    self.state = State::PendingWrite;
                    let writes = self.rebuild(BTreeMap::new());
                    return OfflineCoroutineState::Yielded(OfflineYield::WantsWrite(writes));
                }

                let handles: Vec<Handle> = self.items.iter().map(|i| i.handle.clone()).collect();
                debug!("resolve {} new link ids at meta tier", handles.len());
                self.state = State::PendingFetch;
                OfflineCoroutineState::Yielded(OfflineYield::WantsFetch {
                    collection: self.collection.clone(),
                    handles,
                    tier: Tier::Meta,
                })
            }

            (State::PendingFetch, Some(OfflineArg::Fetch(fetched))) => {
                let links: BTreeMap<Handle, (LinkId, Meta)> = fetched
                    .into_iter()
                    .map(|f| (f.handle, (f.link_id, f.meta)))
                    .collect();

                trace!("resolved {} link ids", links.len());
                self.state = State::PendingWrite;
                let writes = self.rebuild(links);
                OfflineCoroutineState::Yielded(OfflineYield::WantsWrite(writes))
            }

            (State::PendingWrite, Some(OfflineArg::Write)) => {
                debug!(
                    "rekey done: {} carried, {} pulled, {} pending dropped",
                    self.report.rekeyed, self.report.pulled, self.report.dropped,
                );
                OfflineCoroutineState::Complete(Ok(self.report))
            }

            (_, Some(_)) => OfflineCoroutineState::Complete(Err(OfflineRekeyError::UnexpectedArg)),
            (_, None) => OfflineCoroutineState::Complete(Err(OfflineRekeyError::MissingArg)),
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

impl fmt::Display for State {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Start => f.write_str("start"),
            Self::PendingLoad => f.write_str("pending load"),
            Self::PendingEnumerate => f.write_str("pending enumerate"),
            Self::PendingFetch => f.write_str("pending fetch"),
            Self::PendingWrite => f.write_str("pending write"),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        object::Hash,
        placement::Origin,
        rekey::*,
        remote::{FetchedItem, RemoteSnapshot},
        storage::Loaded,
    };

    /// An old-spine placement, synced clean at base `flags`.
    fn synced(handle: &str, link: &str, flags: &[&str]) -> Placement {
        Placement {
            collection: "inbox".into(),
            handle: Handle::from(handle),
            link_id: Some(LinkId::from(link)),
            object: None,
            level: Level::Meta,
            meta: Some(Meta("row".into())),
            flags: Flags::from_iter(flags.iter().copied()),
            status: Status::Clean,
            conflict_revision: None,
            base: Some(Base {
                flags: Flags::from_iter(flags.iter().copied()),
                revision: None,
                object: None,
            }),
            origin: None,
        }
    }

    fn item(handle: &str, flags: &[&str]) -> RemoteItem {
        RemoteItem {
            handle: Handle::from(handle),
            flags: Flags::from_iter(flags.iter().copied()),
            revision: None,
        }
    }

    fn fetched(handle: &str, link: &str) -> FetchedItem {
        FetchedItem {
            handle: Handle::from(handle),
            link_id: LinkId::from(link),
            meta: Meta("fresh row".into()),
            body: None,
            revision: None,
        }
    }

    /// Runs a rekey to completion over the given old spine, new spine and
    /// meta replies, returning the writes and the report.
    fn run(
        old: Vec<Placement>,
        items: Vec<RemoteItem>,
        metas: Vec<FetchedItem>,
    ) -> (Vec<WriteOp>, OfflineRekeyReport) {
        crate::testlog::init();
        let mut rekey = OfflineRekey::new("inbox");
        let _ = rekey.resume(None);
        let _ = rekey.resume(Some(OfflineArg::Load(Loaded {
            placements: old,
            checkpoint: None,
        })));

        let snapshot = RemoteSnapshot {
            items,
            vanished: Vec::new(),
            complete: true,
            checkpoint: Checkpoint(b"v2".to_vec()),
        };
        let writes = match rekey.resume(Some(OfflineArg::Enumerate(snapshot))) {
            OfflineCoroutineState::Yielded(OfflineYield::WantsFetch { tier, .. }) => {
                assert_eq!(tier, Tier::Meta);
                match rekey.resume(Some(OfflineArg::Fetch(metas))) {
                    OfflineCoroutineState::Yielded(OfflineYield::WantsWrite(w)) => w,
                    state => panic!("expected WantsWrite, got {state:?}"),
                }
            }
            OfflineCoroutineState::Yielded(OfflineYield::WantsWrite(w)) => w,
            state => panic!("expected fetch or write, got {state:?}"),
        };

        let report = match rekey.resume(Some(OfflineArg::Write)) {
            OfflineCoroutineState::Complete(Ok(report)) => report,
            state => panic!("expected Complete(Ok), got {state:?}"),
        };
        (writes, report)
    }

    fn upserted<'a>(writes: &'a [WriteOp], handle: &str) -> Option<&'a Placement> {
        writes.iter().find_map(|w| match w {
            WriteOp::UpsertPlacement(p) if p.handle.as_str() == handle => Some(p),
            _ => None,
        })
    }

    #[test]
    fn a_pending_flag_delta_survives_the_bump() {
        // The old placement had staged "flagged" on top of a "seen" base;
        // the new handle re-derives that delta against the new base.
        let mut old = synced("1", "msg-a", &["seen"]);
        old.flags = Flags::from_iter(["seen", "flagged"]);
        old.status = Status::Dirty;

        let (writes, report) = run(
            vec![old],
            vec![item("101", &["seen"])],
            vec![fetched("101", "msg-a")],
        );

        assert_eq!(report.rekeyed, 1);
        assert_eq!(report.dropped, 0);
        assert!(
            writes.iter().any(
                |w| matches!(w, WriteOp::DropPlacement { handle, .. } if handle.as_str() == "1")
            ),
            "the old handle is dropped: {writes:?}",
        );
        let carried = upserted(&writes, "101").expect("a carried placement");
        assert_eq!(carried.status, Status::Dirty, "the delta stays pending");
        assert!(carried.flags.contains("flagged"));
        let base = carried.base.as_ref().expect("a base");
        assert!(base.flags.contains("seen") && !base.flags.contains("flagged"));
    }

    #[test]
    fn a_tombstone_survives_with_its_destination() {
        let mut old = synced("1", "msg-a", &["seen"]);
        old.status = Status::Tombstone;
        old.origin = Some(Origin {
            collection: "archive".into(),
            handle: Handle::from("1"),
        });

        let (writes, report) = run(
            vec![old],
            vec![item("101", &["seen"])],
            vec![fetched("101", "msg-a")],
        );

        assert_eq!(report.rekeyed, 1);
        let carried = upserted(&writes, "101").expect("a carried placement");
        assert_eq!(carried.status, Status::Tombstone);
        assert_eq!(
            carried.origin.as_ref().expect("a move target").collection,
            "archive".into(),
        );
    }

    #[test]
    fn a_staged_edit_survives_with_its_body() {
        let mut old = synced("1", "msg-a", &[]);
        old.object = Some(Hash::from("h2"));
        old.level = Level::Full;
        old.status = Status::Dirty;

        let (writes, report) = run(
            vec![old],
            vec![item("101", &[])],
            vec![fetched("101", "msg-a")],
        );

        assert_eq!(report.rekeyed, 1);
        let carried = upserted(&writes, "101").expect("a carried placement");
        assert_eq!(carried.status, Status::Dirty);
        assert_eq!(carried.object, Some(Hash::from("h2")), "the body survives");
        assert_eq!(carried.level, Level::Full, "the cache survives");
    }

    #[test]
    fn a_clean_cache_carries_over_without_pending_state() {
        let mut old = synced("1", "msg-a", &["seen"]);
        old.object = Some(Hash::from("h1"));
        old.base.as_mut().expect("a base").object = Some(Hash::from("h1"));
        old.level = Level::Full;

        let (writes, report) = run(
            vec![old],
            vec![item("101", &["seen"])],
            vec![fetched("101", "msg-a")],
        );

        assert_eq!(report.rekeyed, 1);
        let carried = upserted(&writes, "101").expect("a carried placement");
        assert_eq!(carried.status, Status::Clean);
        assert_eq!(carried.object, Some(Hash::from("h1")));
        assert_eq!(carried.level, Level::Full);
        let base = carried.base.as_ref().expect("a base");
        assert_eq!(base.object, Some(Hash::from("h1")));
    }

    #[test]
    fn an_unmatched_staged_edit_resurrects_as_a_pending_create() {
        // The edited item is gone from the new spine (deleted during the
        // outage that came with the bump): the edit survives as a pending
        // create, the same edit-beats-delete rule the sync applies.
        let mut old = synced("1", "msg-a", &[]);
        old.object = Some(Hash::from("h2"));
        old.level = Level::Full;
        old.status = Status::Dirty;

        let (writes, report) = run(vec![old], vec![], vec![]);

        assert_eq!(report.rekeyed, 1, "carried as a pending create");
        assert_eq!(report.dropped, 0);
        let resurrected = upserted(&writes, "1").expect("a resurrected placement");
        assert_eq!(resurrected.status, Status::Created);
        assert!(resurrected.base.is_none());
        assert_eq!(resurrected.object, Some(Hash::from("h2")));
    }

    #[test]
    fn unmatched_pending_state_is_dropped_and_counted() {
        // A probed-only placement (no link id resolved) cannot be matched:
        // its pending flag edit is lost with the old handle space.
        let mut old = synced("1", "msg-a", &[]);
        old.link_id = None;
        old.flags = Flags::from_iter(["flagged"]);
        old.status = Status::Dirty;

        let (writes, report) = run(vec![old], vec![item("101", &[])], vec![]);

        assert_eq!(report.rekeyed, 0);
        assert_eq!(report.pulled, 1);
        assert_eq!(report.dropped, 1, "the pending edit is lost, and said so");
        let fresh = upserted(&writes, "101").expect("a fresh placement");
        assert_eq!(fresh.status, Status::Clean);
    }

    #[test]
    fn no_link_ids_skips_the_meta_fetch() {
        let mut old = synced("1", "msg-a", &[]);
        old.link_id = None;

        let mut rekey = OfflineRekey::new("inbox");
        let _ = rekey.resume(None);
        let _ = rekey.resume(Some(OfflineArg::Load(Loaded {
            placements: vec![old],
            checkpoint: None,
        })));

        let snapshot = RemoteSnapshot {
            items: vec![item("101", &[])],
            vanished: Vec::new(),
            complete: true,
            checkpoint: Checkpoint(b"v2".to_vec()),
        };
        match rekey.resume(Some(OfflineArg::Enumerate(snapshot))) {
            OfflineCoroutineState::Yielded(OfflineYield::WantsWrite(_)) => {}
            state => panic!("expected WantsWrite without a fetch, got {state:?}"),
        }
    }

    #[test]
    fn pending_creates_are_left_untouched() {
        let mut placeholder = synced("tmp-1", "msg-b", &[]);
        placeholder.status = Status::Created;
        placeholder.base = None;

        let (writes, report) = run(vec![placeholder], vec![], vec![]);

        assert_eq!(report.rekeyed + report.pulled + report.dropped, 0);
        assert!(
            !writes.iter().any(|w| matches!(
                w,
                WriteOp::DropPlacement { handle, .. } if handle.as_str() == "tmp-1"
            )),
            "the placeholder is not spine, it stays: {writes:?}",
        );
    }

    #[test]
    fn missing_arg_errors() {
        let mut rekey = OfflineRekey::new("inbox");
        let _ = rekey.resume(None);
        match rekey.resume(None) {
            OfflineCoroutineState::Complete(Err(OfflineRekeyError::MissingArg)) => {}
            state => panic!("expected MissingArg, got {state:?}"),
        }
    }

    #[test]
    fn unexpected_arg_errors() {
        let mut rekey = OfflineRekey::new("inbox");
        let _ = rekey.resume(None);
        match rekey.resume(Some(OfflineArg::Write)) {
            OfflineCoroutineState::Complete(Err(OfflineRekeyError::UnexpectedArg)) => {}
            state => panic!("expected UnexpectedArg, got {state:?}"),
        }
    }
}
