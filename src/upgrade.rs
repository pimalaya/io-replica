//! I/O-free coroutine to raise placements to a higher detail level.
//!
//! A pure pull, never a merge. It loads the targeted placements, then for
//! [`ReplicaTier::Full`] resolves their link ids against the object store first:
//! a body already stored under another collection is linked without any
//! network round-trip. This stores one body for an item that appears in
//! several collections at once, and backs a unified across-collections
//! view.

use core::{fmt, mem};

use alloc::{collections::BTreeMap, vec::Vec};

use log::{debug, trace};
use thiserror::Error;

use crate::{
    change::ReplicaWriteOp,
    collection::ReplicaCollectionId,
    coroutine::*,
    object::ReplicaObject,
    placement::{ReplicaHandle, ReplicaLevel, ReplicaPlacement},
    remote::ReplicaTier,
};

/// What an upgrade did.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ReplicaUpgradeReport {
    /// Placements raised to the requested level.
    pub upgraded: usize,
    /// Bodies fetched from the remote.
    pub fetched: usize,
    /// Bodies linked from the object store without a fetch.
    pub deduped: usize,
}

/// Failure causes during an UPGRADE flow.
#[derive(Clone, Debug, Error)]
pub enum ReplicaUpgradeError {
    /// The driver fed back an arg that does not match the pending yield.
    #[error("Offline UPGRADE failed: unexpected coroutine arg")]
    UnexpectedArg,
    /// The driver resumed without the arg the pending yield required.
    #[error("Offline UPGRADE failed: missing coroutine arg")]
    MissingArg,
}

/// I/O-free UPGRADE coroutine.
pub struct ReplicaUpgrade {
    collection: ReplicaCollectionId,
    handles: Vec<ReplicaHandle>,
    tier: ReplicaTier,
    placements: BTreeMap<ReplicaHandle, ReplicaPlacement>,
    ops: Vec<ReplicaWriteOp>,
    report: ReplicaUpgradeReport,
    state: State,
}

impl ReplicaUpgrade {
    /// Creates a coroutine that raises `handles` in `collection` to
    /// `tier`.
    pub fn new(
        collection: impl Into<ReplicaCollectionId>,
        handles: Vec<ReplicaHandle>,
        tier: ReplicaTier,
    ) -> Self {
        let collection = collection.into();
        debug!(
            "upgrade {} handles in {} to {tier:?}",
            handles.len(),
            collection.as_str(),
        );

        Self {
            collection,
            handles,
            tier,
            placements: BTreeMap::new(),
            ops: Vec::new(),
            report: ReplicaUpgradeReport::default(),
            state: State::Start,
        }
    }

    /// Requested handles that still need work for the target tier.
    fn pending_handles(&self) -> Vec<ReplicaHandle> {
        self.handles
            .iter()
            .filter(|h| match self.placements.get(h) {
                Some(p) => match self.tier {
                    ReplicaTier::Meta => p.level < ReplicaLevel::Meta,
                    ReplicaTier::Full => p.level < ReplicaLevel::Full,
                },
                None => false,
            })
            .cloned()
            .collect()
    }
}

impl ReplicaCoroutine for ReplicaUpgrade {
    type Yield = ReplicaYield;
    type Return = Result<ReplicaUpgradeReport, ReplicaUpgradeError>;

    fn resume(
        &mut self,
        arg: Option<ReplicaArg>,
    ) -> ReplicaCoroutineState<Self::Yield, Self::Return> {
        trace!("upgrade: {}", self.state);

        match (&self.state, arg) {
            (State::Start, None) => {
                debug!("load target items from storage");
                self.state = State::PendingLoad;
                ReplicaCoroutineState::Yielded(ReplicaYield::WantsLoad(self.collection.clone()))
            }

            (State::PendingLoad, Some(ReplicaArg::Load(loaded))) => {
                self.placements = loaded
                    .placements
                    .into_iter()
                    .map(|p| (p.handle.clone(), p))
                    .collect();

                let pending = self.pending_handles();
                if pending.is_empty() {
                    debug!("nothing to upgrade");
                    return ReplicaCoroutineState::Complete(Ok(self.report));
                }

                match self.tier {
                    ReplicaTier::Meta => {
                        debug!("fetch {} items at meta tier", pending.len());
                        self.state = State::PendingFetch;
                        ReplicaCoroutineState::Yielded(ReplicaYield::WantsFetch {
                            collection: self.collection.clone(),
                            handles: pending,
                            tier: ReplicaTier::Meta,
                        })
                    }
                    ReplicaTier::Full => {
                        let links: Vec<_> = pending
                            .iter()
                            .filter_map(|h| self.placements.get(h))
                            .filter_map(|p| p.link_id.clone())
                            .collect();

                        if links.is_empty() {
                            debug!("fetch {} items at full tier", pending.len());
                            self.state = State::PendingFetch;
                            return ReplicaCoroutineState::Yielded(ReplicaYield::WantsFetch {
                                collection: self.collection.clone(),
                                handles: pending,
                                tier: ReplicaTier::Full,
                            });
                        }

                        debug!("look up {} link ids in object store", links.len());
                        trace!("link ids: {links:?}");
                        self.state = State::PendingLookup;
                        ReplicaCoroutineState::Yielded(ReplicaYield::WantsLookupObject(links))
                    }
                }
            }

            (State::PendingLookup, Some(ReplicaArg::LookupObject(known))) => {
                let mut to_fetch = Vec::new();

                for handle in self.pending_handles() {
                    let Some(placement) = self.placements.get(&handle) else {
                        continue;
                    };
                    let hit = placement
                        .link_id
                        .as_ref()
                        .and_then(|link| known.get(link).cloned());

                    match hit {
                        Some(hash) => {
                            let mut patched = placement.clone();
                            patched.object = Some(hash);
                            patched.level = ReplicaLevel::Full;
                            self.ops.push(ReplicaWriteOp::UpsertPlacement(patched));
                            self.report.upgraded += 1;
                            self.report.deduped += 1;
                        }
                        None => to_fetch.push(handle),
                    }
                }

                if to_fetch.is_empty() {
                    debug!("linked {} bodies from store, no fetch", self.report.deduped);
                    self.state = State::PendingWrite;
                    let ops = mem::take(&mut self.ops);
                    return ReplicaCoroutineState::Yielded(ReplicaYield::WantsWrite(ops));
                }

                debug!(
                    "fetch {} bodies, {} linked from store",
                    to_fetch.len(),
                    self.report.deduped,
                );
                self.state = State::PendingFetch;
                ReplicaCoroutineState::Yielded(ReplicaYield::WantsFetch {
                    collection: self.collection.clone(),
                    handles: to_fetch,
                    tier: ReplicaTier::Full,
                })
            }

            (State::PendingFetch, Some(ReplicaArg::Fetch(items))) => {
                trace!("fetched {} items", items.len());
                for item in items {
                    let Some(placement) = self.placements.get(&item.handle) else {
                        continue;
                    };
                    let mut patched = placement.clone();
                    patched.link_id = Some(item.link_id);
                    patched.meta = Some(item.meta);

                    match (self.tier, item.body) {
                        (ReplicaTier::Full, Some((hash, body))) => {
                            let object = ReplicaObject {
                                hash: hash.clone(),
                                size: body.len(),
                            };
                            self.ops.push(ReplicaWriteOp::StoreObject { object, body });

                            // NOTE: the revision travels with the body: the
                            // stored object is the remote content as of the
                            // fetch, so the base records both.
                            if let Some(base) = &mut patched.base {
                                base.revision = item.revision.clone();
                                base.object = Some(hash.clone());
                            }

                            patched.object = Some(hash);
                            patched.level = ReplicaLevel::Full;
                            self.report.fetched += 1;
                        }
                        _ => {
                            patched.level = ReplicaLevel::Meta;
                            self.report.fetched += 1;
                        }
                    }

                    self.ops.push(ReplicaWriteOp::UpsertPlacement(patched));
                    self.report.upgraded += 1;
                }

                debug!("write {} storage ops", self.ops.len());
                self.state = State::PendingWrite;
                let ops = mem::take(&mut self.ops);
                ReplicaCoroutineState::Yielded(ReplicaYield::WantsWrite(ops))
            }

            (State::PendingWrite, Some(ReplicaArg::Write)) => {
                debug!(
                    "upgraded {} items ({} fetched, {} linked from store)",
                    self.report.upgraded, self.report.fetched, self.report.deduped,
                );
                ReplicaCoroutineState::Complete(Ok(self.report))
            }

            (_, Some(_)) => {
                ReplicaCoroutineState::Complete(Err(ReplicaUpgradeError::UnexpectedArg))
            }
            (_, None) => ReplicaCoroutineState::Complete(Err(ReplicaUpgradeError::MissingArg)),
        }
    }
}

enum State {
    Start,
    PendingLoad,
    PendingLookup,
    PendingFetch,
    PendingWrite,
}

impl fmt::Display for State {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Start => f.write_str("start"),
            Self::PendingLoad => f.write_str("pending load"),
            Self::PendingLookup => f.write_str("pending object lookup"),
            Self::PendingFetch => f.write_str("pending fetch"),
            Self::PendingWrite => f.write_str("pending write"),
        }
    }
}

#[cfg(test)]
mod tests {
    use alloc::{collections::BTreeMap, vec};

    use crate::{
        object::ReplicaHash,
        placement::{ReplicaBase, ReplicaFlags, ReplicaLinkId, ReplicaMeta, ReplicaStatus},
        remote::ReplicaFetchedItem,
        storage::ReplicaLoaded,
        upgrade::*,
    };

    fn probed(handle: &str, link: Option<&str>, level: ReplicaLevel) -> ReplicaPlacement {
        ReplicaPlacement {
            collection: "inbox".into(),
            handle: ReplicaHandle::from(handle),
            link_id: link.map(ReplicaLinkId::from),
            object: None,
            level,
            meta: None,
            flags: ReplicaFlags::default(),
            status: ReplicaStatus::Clean,
            conflict_revision: None,
            base: None,
            origin: None,
        }
    }

    #[test]
    fn full_dedup_links_without_fetch() {
        crate::testlog::init();
        let loaded = ReplicaLoaded {
            placements: vec![probed("2", Some("msg-a"), ReplicaLevel::Meta)],
            checkpoint: None,
        };
        let mut up =
            ReplicaUpgrade::new("inbox", vec![ReplicaHandle::from("2")], ReplicaTier::Full);
        let _ = up.resume(None);

        let links = match up.resume(Some(ReplicaArg::Load(loaded))) {
            ReplicaCoroutineState::Yielded(ReplicaYield::WantsLookupObject(links)) => links,
            state => panic!("expected WantsLookupObject, got {state:?}"),
        };
        assert_eq!(links, vec![ReplicaLinkId::from("msg-a")]);

        let mut known = BTreeMap::new();
        known.insert(ReplicaLinkId::from("msg-a"), ReplicaHash::from("h-a"));

        let ops = match up.resume(Some(ReplicaArg::LookupObject(known))) {
            ReplicaCoroutineState::Yielded(ReplicaYield::WantsWrite(ops)) => ops,
            state => panic!("expected WantsWrite (no fetch), got {state:?}"),
        };
        let ReplicaWriteOp::UpsertPlacement(p) = &ops[0] else {
            panic!("expected UpsertPlacement, got {:?}", ops[0]);
        };
        assert_eq!(p.level, ReplicaLevel::Full);
        assert_eq!(p.object, Some(ReplicaHash::from("h-a")));

        let report = match up.resume(Some(ReplicaArg::Write)) {
            ReplicaCoroutineState::Complete(Ok(report)) => report,
            state => panic!("expected Complete(Ok), got {state:?}"),
        };
        assert_eq!(report.deduped, 1);
        assert_eq!(report.fetched, 0);
    }

    #[test]
    fn full_miss_fetches_and_stores() {
        let loaded = ReplicaLoaded {
            placements: vec![probed("1", Some("msg-b"), ReplicaLevel::Meta)],
            checkpoint: None,
        };
        let mut up =
            ReplicaUpgrade::new("inbox", vec![ReplicaHandle::from("1")], ReplicaTier::Full);
        let _ = up.resume(None);
        let _ = up.resume(Some(ReplicaArg::Load(loaded)));

        let handles = match up.resume(Some(ReplicaArg::LookupObject(BTreeMap::new()))) {
            ReplicaCoroutineState::Yielded(ReplicaYield::WantsFetch { handles, tier, .. }) => {
                assert_eq!(tier, ReplicaTier::Full);
                handles
            }
            state => panic!("expected WantsFetch, got {state:?}"),
        };
        assert_eq!(handles, vec![ReplicaHandle::from("1")]);

        let items = vec![ReplicaFetchedItem {
            handle: ReplicaHandle::from("1"),
            link_id: ReplicaLinkId::from("msg-b"),
            meta: ReplicaMeta("hdr".into()),
            body: Some((ReplicaHash::from("h-b"), b"body".to_vec())),
            revision: None,
        }];
        let ops = match up.resume(Some(ReplicaArg::Fetch(items))) {
            ReplicaCoroutineState::Yielded(ReplicaYield::WantsWrite(ops)) => ops,
            state => panic!("expected WantsWrite, got {state:?}"),
        };
        assert!(matches!(ops[0], ReplicaWriteOp::StoreObject { .. }));

        let report = match up.resume(Some(ReplicaArg::Write)) {
            ReplicaCoroutineState::Complete(Ok(report)) => report,
            state => panic!("expected Complete(Ok), got {state:?}"),
        };
        assert_eq!(report.fetched, 1);
        assert_eq!(report.deduped, 0);
    }

    #[test]
    fn full_fetch_stamps_the_base_revision_and_object() {
        // A fetched body is the remote content as of the fetch: a based
        // placement records the fetched revision and pins the stored body
        // as its content base.
        let mut placement = probed("1", Some("msg-b"), ReplicaLevel::Meta);
        placement.base = Some(ReplicaBase {
            flags: ReplicaFlags::default(),
            revision: None,
            object: None,
        });
        let loaded = ReplicaLoaded {
            placements: vec![placement],
            checkpoint: None,
        };

        let mut up =
            ReplicaUpgrade::new("inbox", vec![ReplicaHandle::from("1")], ReplicaTier::Full);
        let _ = up.resume(None);
        let _ = up.resume(Some(ReplicaArg::Load(loaded)));
        let _ = up.resume(Some(ReplicaArg::LookupObject(BTreeMap::new())));

        let items = vec![ReplicaFetchedItem {
            handle: ReplicaHandle::from("1"),
            link_id: ReplicaLinkId::from("msg-b"),
            meta: ReplicaMeta("hdr".into()),
            body: Some((ReplicaHash::from("h-b"), b"body".to_vec())),
            revision: Some("r7".into()),
        }];
        let ops = match up.resume(Some(ReplicaArg::Fetch(items))) {
            ReplicaCoroutineState::Yielded(ReplicaYield::WantsWrite(ops)) => ops,
            state => panic!("expected WantsWrite, got {state:?}"),
        };

        let patched = ops
            .iter()
            .find_map(|op| match op {
                ReplicaWriteOp::UpsertPlacement(p) => Some(p),
                _ => None,
            })
            .expect("an upserted placement");
        let base = patched.base.as_ref().expect("a base");
        assert_eq!(base.revision.as_deref(), Some("r7"));
        assert_eq!(base.object, Some(ReplicaHash::from("h-b")));
        assert_eq!(patched.object, Some(ReplicaHash::from("h-b")));
    }

    #[test]
    fn meta_upgrade_fetches_headers() {
        let loaded = ReplicaLoaded {
            placements: vec![probed("1", None, ReplicaLevel::Probed)],
            checkpoint: None,
        };
        let mut up =
            ReplicaUpgrade::new("inbox", vec![ReplicaHandle::from("1")], ReplicaTier::Meta);
        let _ = up.resume(None);

        match up.resume(Some(ReplicaArg::Load(loaded))) {
            ReplicaCoroutineState::Yielded(ReplicaYield::WantsFetch { tier, .. }) => {
                assert_eq!(tier, ReplicaTier::Meta);
            }
            state => panic!("expected WantsFetch ReplicaMeta, got {state:?}"),
        }
    }

    #[test]
    fn already_full_completes_without_work() {
        let loaded = ReplicaLoaded {
            placements: vec![probed("1", Some("x"), ReplicaLevel::Full)],
            checkpoint: None,
        };
        let mut up =
            ReplicaUpgrade::new("inbox", vec![ReplicaHandle::from("1")], ReplicaTier::Full);
        let _ = up.resume(None);

        match up.resume(Some(ReplicaArg::Load(loaded))) {
            ReplicaCoroutineState::Complete(Ok(report)) => assert_eq!(report.upgraded, 0),
            state => panic!("expected Complete(Ok), got {state:?}"),
        }
    }

    #[test]
    fn missing_arg_errors() {
        let mut up =
            ReplicaUpgrade::new("inbox", vec![ReplicaHandle::from("1")], ReplicaTier::Meta);
        let _ = up.resume(None);
        match up.resume(None) {
            ReplicaCoroutineState::Complete(Err(ReplicaUpgradeError::MissingArg)) => {}
            state => panic!("expected MissingArg, got {state:?}"),
        }
    }

    #[test]
    fn unexpected_arg_errors() {
        let mut up =
            ReplicaUpgrade::new("inbox", vec![ReplicaHandle::from("1")], ReplicaTier::Meta);
        let _ = up.resume(None);
        match up.resume(Some(ReplicaArg::Write)) {
            ReplicaCoroutineState::Complete(Err(ReplicaUpgradeError::UnexpectedArg)) => {}
            state => panic!("expected UnexpectedArg, got {state:?}"),
        }
    }

    #[test]
    fn unknown_handle_completes_without_work() {
        // A requested handle with no placement is not the upgrade's to
        // invent: it is skipped, not fetched.
        let loaded = ReplicaLoaded {
            placements: vec![probed("1", None, ReplicaLevel::Probed)],
            checkpoint: None,
        };
        let mut up = ReplicaUpgrade::new(
            "inbox",
            vec![ReplicaHandle::from("nope")],
            ReplicaTier::Meta,
        );
        let _ = up.resume(None);

        match up.resume(Some(ReplicaArg::Load(loaded))) {
            ReplicaCoroutineState::Complete(Ok(report)) => assert_eq!(report.upgraded, 0),
            state => panic!("expected Complete(Ok), got {state:?}"),
        }
    }

    #[test]
    fn full_without_link_ids_fetches_directly() {
        // Probed placements carry no link id yet, so there is nothing to
        // look up in the object store: the full upgrade fetches directly.
        let loaded = ReplicaLoaded {
            placements: vec![probed("1", None, ReplicaLevel::Probed)],
            checkpoint: None,
        };
        let mut up =
            ReplicaUpgrade::new("inbox", vec![ReplicaHandle::from("1")], ReplicaTier::Full);
        let _ = up.resume(None);

        match up.resume(Some(ReplicaArg::Load(loaded))) {
            ReplicaCoroutineState::Yielded(ReplicaYield::WantsFetch { tier, handles, .. }) => {
                assert_eq!(tier, ReplicaTier::Full);
                assert_eq!(handles, vec![ReplicaHandle::from("1")]);
            }
            state => panic!("expected WantsFetch Full, got {state:?}"),
        }
    }

    #[test]
    fn fetched_unknown_handle_is_skipped() {
        // A fetch reply naming a handle with no placement (a lagging or
        // buggy consumer) is ignored rather than upserted or panicking.
        let loaded = ReplicaLoaded {
            placements: vec![probed("1", None, ReplicaLevel::Probed)],
            checkpoint: None,
        };
        let mut up =
            ReplicaUpgrade::new("inbox", vec![ReplicaHandle::from("1")], ReplicaTier::Meta);
        let _ = up.resume(None);
        let _ = up.resume(Some(ReplicaArg::Load(loaded)));

        let items = vec![ReplicaFetchedItem {
            handle: ReplicaHandle::from("ghost"),
            link_id: ReplicaLinkId::from("msg-x"),
            meta: ReplicaMeta("hdr".into()),
            body: None,
            revision: None,
        }];
        let ops = match up.resume(Some(ReplicaArg::Fetch(items))) {
            ReplicaCoroutineState::Yielded(ReplicaYield::WantsWrite(ops)) => ops,
            state => panic!("expected WantsWrite, got {state:?}"),
        };
        assert!(ops.is_empty(), "nothing to write: {ops:?}");

        match up.resume(Some(ReplicaArg::Write)) {
            ReplicaCoroutineState::Complete(Ok(report)) => assert_eq!(report.upgraded, 0),
            state => panic!("expected Complete(Ok), got {state:?}"),
        }
    }

    #[test]
    fn full_mixes_dedup_hits_and_fetch_misses() {
        crate::testlog::init();
        // Two placements at meta level: one link id resolves in the object
        // store (linked without a fetch), the other misses and is fetched.
        let loaded = ReplicaLoaded {
            placements: vec![
                probed("1", Some("msg-a"), ReplicaLevel::Meta),
                probed("2", Some("msg-b"), ReplicaLevel::Meta),
            ],
            checkpoint: None,
        };
        let mut up = ReplicaUpgrade::new(
            "inbox",
            vec![ReplicaHandle::from("1"), ReplicaHandle::from("2")],
            ReplicaTier::Full,
        );
        let _ = up.resume(None);
        let _ = up.resume(Some(ReplicaArg::Load(loaded)));

        let mut known = BTreeMap::new();
        known.insert(ReplicaLinkId::from("msg-a"), ReplicaHash::from("h-a"));

        let handles = match up.resume(Some(ReplicaArg::LookupObject(known))) {
            ReplicaCoroutineState::Yielded(ReplicaYield::WantsFetch { handles, .. }) => handles,
            state => panic!("expected WantsFetch for the miss, got {state:?}"),
        };
        assert_eq!(
            handles,
            vec![ReplicaHandle::from("2")],
            "only the miss fetches"
        );

        let items = vec![ReplicaFetchedItem {
            handle: ReplicaHandle::from("2"),
            link_id: ReplicaLinkId::from("msg-b"),
            meta: ReplicaMeta("hdr".into()),
            body: Some((ReplicaHash::from("h-b"), b"body".to_vec())),
            revision: None,
        }];
        let _ = up.resume(Some(ReplicaArg::Fetch(items)));

        let report = match up.resume(Some(ReplicaArg::Write)) {
            ReplicaCoroutineState::Complete(Ok(report)) => report,
            state => panic!("expected Complete(Ok), got {state:?}"),
        };
        assert_eq!(report.upgraded, 2);
        assert_eq!(report.deduped, 1);
        assert_eq!(report.fetched, 1);
    }
}
