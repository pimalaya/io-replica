//! I/O-free coroutine to raise placements to a higher detail level.
//!
//! A pure pull, never a merge. It loads the targeted placements, then for
//! [`Tier::Full`] resolves their link ids against the object store first:
//! a body already stored under another collection is linked without any
//! network round-trip. This stores one body for an item that appears in
//! several collections at once, and backs a unified across-collections
//! view.

use core::{fmt, mem};

use alloc::{collections::BTreeMap, vec::Vec};

use log::{debug, trace};
use thiserror::Error;

use crate::{
    change::WriteOp,
    collection::CollectionId,
    coroutine::*,
    object::Object,
    placement::{Handle, Level, Placement},
    remote::Tier,
};

/// What an upgrade did.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct OfflineUpgradeReport {
    /// Placements raised to the requested level.
    pub upgraded: usize,
    /// Bodies fetched from the remote.
    pub fetched: usize,
    /// Bodies linked from the object store without a fetch.
    pub deduped: usize,
}

/// Failure causes during an UPGRADE flow.
#[derive(Clone, Debug, Error)]
pub enum OfflineUpgradeError {
    /// The driver fed back an arg that does not match the pending yield.
    #[error("Offline UPGRADE failed: unexpected coroutine arg")]
    UnexpectedArg,
    /// The driver resumed without the arg the pending yield required.
    #[error("Offline UPGRADE failed: missing coroutine arg")]
    MissingArg,
}

/// I/O-free UPGRADE coroutine.
pub struct OfflineUpgrade {
    collection: CollectionId,
    handles: Vec<Handle>,
    tier: Tier,
    placements: BTreeMap<Handle, Placement>,
    ops: Vec<WriteOp>,
    report: OfflineUpgradeReport,
    state: State,
}

impl OfflineUpgrade {
    /// Creates a coroutine that raises `handles` in `collection` to
    /// `tier`.
    pub fn new(collection: impl Into<CollectionId>, handles: Vec<Handle>, tier: Tier) -> Self {
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
            report: OfflineUpgradeReport::default(),
            state: State::Start,
        }
    }

    /// Requested handles that still need work for the target tier.
    fn pending_handles(&self) -> Vec<Handle> {
        self.handles
            .iter()
            .filter(|h| match self.placements.get(h) {
                Some(p) => match self.tier {
                    Tier::Meta => p.level < Level::Meta,
                    Tier::Full => p.level < Level::Full,
                },
                None => false,
            })
            .cloned()
            .collect()
    }
}

impl OfflineCoroutine for OfflineUpgrade {
    type Yield = OfflineYield;
    type Return = Result<OfflineUpgradeReport, OfflineUpgradeError>;

    fn resume(
        &mut self,
        arg: Option<OfflineArg>,
    ) -> OfflineCoroutineState<Self::Yield, Self::Return> {
        trace!("upgrade: {}", self.state);

        match (&self.state, arg) {
            (State::Start, None) => {
                debug!("load target items from storage");
                self.state = State::PendingLoad;
                OfflineCoroutineState::Yielded(OfflineYield::WantsLoad(self.collection.clone()))
            }

            (State::PendingLoad, Some(OfflineArg::Load(loaded))) => {
                self.placements = loaded
                    .placements
                    .into_iter()
                    .map(|p| (p.handle.clone(), p))
                    .collect();

                let pending = self.pending_handles();
                if pending.is_empty() {
                    debug!("nothing to upgrade");
                    return OfflineCoroutineState::Complete(Ok(self.report));
                }

                match self.tier {
                    Tier::Meta => {
                        debug!("fetch {} items at meta tier", pending.len());
                        self.state = State::PendingFetch;
                        OfflineCoroutineState::Yielded(OfflineYield::WantsFetch {
                            collection: self.collection.clone(),
                            handles: pending,
                            tier: Tier::Meta,
                        })
                    }
                    Tier::Full => {
                        let links: Vec<_> = pending
                            .iter()
                            .filter_map(|h| self.placements.get(h))
                            .filter_map(|p| p.link_id.clone())
                            .collect();

                        if links.is_empty() {
                            debug!("fetch {} items at full tier", pending.len());
                            self.state = State::PendingFetch;
                            return OfflineCoroutineState::Yielded(OfflineYield::WantsFetch {
                                collection: self.collection.clone(),
                                handles: pending,
                                tier: Tier::Full,
                            });
                        }

                        debug!("look up {} link ids in object store", links.len());
                        trace!("link ids: {links:?}");
                        self.state = State::PendingLookup;
                        OfflineCoroutineState::Yielded(OfflineYield::WantsLookupObject(links))
                    }
                }
            }

            (State::PendingLookup, Some(OfflineArg::LookupObject(known))) => {
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
                            patched.level = Level::Full;
                            self.ops.push(WriteOp::UpsertPlacement(patched));
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
                    return OfflineCoroutineState::Yielded(OfflineYield::WantsWrite(ops));
                }

                debug!(
                    "fetch {} bodies, {} linked from store",
                    to_fetch.len(),
                    self.report.deduped,
                );
                self.state = State::PendingFetch;
                OfflineCoroutineState::Yielded(OfflineYield::WantsFetch {
                    collection: self.collection.clone(),
                    handles: to_fetch,
                    tier: Tier::Full,
                })
            }

            (State::PendingFetch, Some(OfflineArg::Fetch(items))) => {
                trace!("fetched {} items", items.len());
                for item in items {
                    let Some(placement) = self.placements.get(&item.handle) else {
                        continue;
                    };
                    let mut patched = placement.clone();
                    patched.link_id = Some(item.link_id);
                    patched.meta = Some(item.meta);

                    match (self.tier, item.body) {
                        (Tier::Full, Some((hash, body))) => {
                            let object = Object {
                                hash: hash.clone(),
                                size: body.len(),
                            };
                            self.ops.push(WriteOp::StoreObject { object, body });

                            // NOTE: the revision travels with the body: the
                            // stored object is the remote content as of the
                            // fetch, so the base records both.
                            if let Some(base) = &mut patched.base {
                                base.revision = item.revision.clone();
                                base.object = Some(hash.clone());
                            }

                            patched.object = Some(hash);
                            patched.level = Level::Full;
                            self.report.fetched += 1;
                        }
                        _ => {
                            patched.level = Level::Meta;
                            self.report.fetched += 1;
                        }
                    }

                    self.ops.push(WriteOp::UpsertPlacement(patched));
                    self.report.upgraded += 1;
                }

                debug!("write {} storage ops", self.ops.len());
                self.state = State::PendingWrite;
                let ops = mem::take(&mut self.ops);
                OfflineCoroutineState::Yielded(OfflineYield::WantsWrite(ops))
            }

            (State::PendingWrite, Some(OfflineArg::Write)) => {
                debug!(
                    "upgraded {} items ({} fetched, {} linked from store)",
                    self.report.upgraded, self.report.fetched, self.report.deduped,
                );
                OfflineCoroutineState::Complete(Ok(self.report))
            }

            (_, Some(_)) => {
                OfflineCoroutineState::Complete(Err(OfflineUpgradeError::UnexpectedArg))
            }
            (_, None) => OfflineCoroutineState::Complete(Err(OfflineUpgradeError::MissingArg)),
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
    use alloc::collections::BTreeMap;

    use crate::{
        object::Hash,
        placement::{Base, Flags, LinkId, Meta, Status},
        remote::FetchedItem,
        storage::Loaded,
        upgrade::*,
    };

    fn probed(handle: &str, link: Option<&str>, level: Level) -> Placement {
        Placement {
            collection: "inbox".into(),
            handle: Handle::from(handle),
            link_id: link.map(LinkId::from),
            object: None,
            level,
            meta: None,
            flags: Flags::default(),
            status: Status::Clean,
            conflict_revision: None,
            base: None,
            origin: None,
        }
    }

    #[test]
    fn full_dedup_links_without_fetch() {
        crate::testlog::init();
        let loaded = Loaded {
            placements: vec![probed("2", Some("msg-a"), Level::Meta)],
            checkpoint: None,
        };
        let mut up = OfflineUpgrade::new("inbox", vec![Handle::from("2")], Tier::Full);
        let _ = up.resume(None);

        let links = match up.resume(Some(OfflineArg::Load(loaded))) {
            OfflineCoroutineState::Yielded(OfflineYield::WantsLookupObject(links)) => links,
            state => panic!("expected WantsLookupObject, got {state:?}"),
        };
        assert_eq!(links, vec![LinkId::from("msg-a")]);

        let mut known = BTreeMap::new();
        known.insert(LinkId::from("msg-a"), Hash::from("h-a"));

        let ops = match up.resume(Some(OfflineArg::LookupObject(known))) {
            OfflineCoroutineState::Yielded(OfflineYield::WantsWrite(ops)) => ops,
            state => panic!("expected WantsWrite (no fetch), got {state:?}"),
        };
        let WriteOp::UpsertPlacement(p) = &ops[0] else {
            panic!("expected UpsertPlacement, got {:?}", ops[0]);
        };
        assert_eq!(p.level, Level::Full);
        assert_eq!(p.object, Some(Hash::from("h-a")));

        let report = match up.resume(Some(OfflineArg::Write)) {
            OfflineCoroutineState::Complete(Ok(report)) => report,
            state => panic!("expected Complete(Ok), got {state:?}"),
        };
        assert_eq!(report.deduped, 1);
        assert_eq!(report.fetched, 0);
    }

    #[test]
    fn full_miss_fetches_and_stores() {
        let loaded = Loaded {
            placements: vec![probed("1", Some("msg-b"), Level::Meta)],
            checkpoint: None,
        };
        let mut up = OfflineUpgrade::new("inbox", vec![Handle::from("1")], Tier::Full);
        let _ = up.resume(None);
        let _ = up.resume(Some(OfflineArg::Load(loaded)));

        let handles = match up.resume(Some(OfflineArg::LookupObject(BTreeMap::new()))) {
            OfflineCoroutineState::Yielded(OfflineYield::WantsFetch { handles, tier, .. }) => {
                assert_eq!(tier, Tier::Full);
                handles
            }
            state => panic!("expected WantsFetch, got {state:?}"),
        };
        assert_eq!(handles, vec![Handle::from("1")]);

        let items = vec![FetchedItem {
            handle: Handle::from("1"),
            link_id: LinkId::from("msg-b"),
            meta: Meta("hdr".into()),
            body: Some((Hash::from("h-b"), b"body".to_vec())),
            revision: None,
        }];
        let ops = match up.resume(Some(OfflineArg::Fetch(items))) {
            OfflineCoroutineState::Yielded(OfflineYield::WantsWrite(ops)) => ops,
            state => panic!("expected WantsWrite, got {state:?}"),
        };
        assert!(matches!(ops[0], WriteOp::StoreObject { .. }));

        let report = match up.resume(Some(OfflineArg::Write)) {
            OfflineCoroutineState::Complete(Ok(report)) => report,
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
        let mut placement = probed("1", Some("msg-b"), Level::Meta);
        placement.base = Some(Base {
            flags: Flags::default(),
            revision: None,
            object: None,
        });
        let loaded = Loaded {
            placements: vec![placement],
            checkpoint: None,
        };

        let mut up = OfflineUpgrade::new("inbox", vec![Handle::from("1")], Tier::Full);
        let _ = up.resume(None);
        let _ = up.resume(Some(OfflineArg::Load(loaded)));
        let _ = up.resume(Some(OfflineArg::LookupObject(BTreeMap::new())));

        let items = vec![FetchedItem {
            handle: Handle::from("1"),
            link_id: LinkId::from("msg-b"),
            meta: Meta("hdr".into()),
            body: Some((Hash::from("h-b"), b"body".to_vec())),
            revision: Some("r7".into()),
        }];
        let ops = match up.resume(Some(OfflineArg::Fetch(items))) {
            OfflineCoroutineState::Yielded(OfflineYield::WantsWrite(ops)) => ops,
            state => panic!("expected WantsWrite, got {state:?}"),
        };

        let patched = ops
            .iter()
            .find_map(|op| match op {
                WriteOp::UpsertPlacement(p) => Some(p),
                _ => None,
            })
            .expect("an upserted placement");
        let base = patched.base.as_ref().expect("a base");
        assert_eq!(base.revision.as_deref(), Some("r7"));
        assert_eq!(base.object, Some(Hash::from("h-b")));
        assert_eq!(patched.object, Some(Hash::from("h-b")));
    }

    #[test]
    fn meta_upgrade_fetches_headers() {
        let loaded = Loaded {
            placements: vec![probed("1", None, Level::Probed)],
            checkpoint: None,
        };
        let mut up = OfflineUpgrade::new("inbox", vec![Handle::from("1")], Tier::Meta);
        let _ = up.resume(None);

        match up.resume(Some(OfflineArg::Load(loaded))) {
            OfflineCoroutineState::Yielded(OfflineYield::WantsFetch { tier, .. }) => {
                assert_eq!(tier, Tier::Meta);
            }
            state => panic!("expected WantsFetch Meta, got {state:?}"),
        }
    }

    #[test]
    fn already_full_completes_without_work() {
        let loaded = Loaded {
            placements: vec![probed("1", Some("x"), Level::Full)],
            checkpoint: None,
        };
        let mut up = OfflineUpgrade::new("inbox", vec![Handle::from("1")], Tier::Full);
        let _ = up.resume(None);

        match up.resume(Some(OfflineArg::Load(loaded))) {
            OfflineCoroutineState::Complete(Ok(report)) => assert_eq!(report.upgraded, 0),
            state => panic!("expected Complete(Ok), got {state:?}"),
        }
    }

    #[test]
    fn missing_arg_errors() {
        let mut up = OfflineUpgrade::new("inbox", vec![Handle::from("1")], Tier::Meta);
        let _ = up.resume(None);
        match up.resume(None) {
            OfflineCoroutineState::Complete(Err(OfflineUpgradeError::MissingArg)) => {}
            state => panic!("expected MissingArg, got {state:?}"),
        }
    }

    #[test]
    fn unexpected_arg_errors() {
        let mut up = OfflineUpgrade::new("inbox", vec![Handle::from("1")], Tier::Meta);
        let _ = up.resume(None);
        match up.resume(Some(OfflineArg::Write)) {
            OfflineCoroutineState::Complete(Err(OfflineUpgradeError::UnexpectedArg)) => {}
            state => panic!("expected UnexpectedArg, got {state:?}"),
        }
    }

    #[test]
    fn unknown_handle_completes_without_work() {
        // A requested handle with no placement is not the upgrade's to
        // invent: it is skipped, not fetched.
        let loaded = Loaded {
            placements: vec![probed("1", None, Level::Probed)],
            checkpoint: None,
        };
        let mut up = OfflineUpgrade::new("inbox", vec![Handle::from("nope")], Tier::Meta);
        let _ = up.resume(None);

        match up.resume(Some(OfflineArg::Load(loaded))) {
            OfflineCoroutineState::Complete(Ok(report)) => assert_eq!(report.upgraded, 0),
            state => panic!("expected Complete(Ok), got {state:?}"),
        }
    }

    #[test]
    fn full_without_link_ids_fetches_directly() {
        // Probed placements carry no link id yet, so there is nothing to
        // look up in the object store: the full upgrade fetches directly.
        let loaded = Loaded {
            placements: vec![probed("1", None, Level::Probed)],
            checkpoint: None,
        };
        let mut up = OfflineUpgrade::new("inbox", vec![Handle::from("1")], Tier::Full);
        let _ = up.resume(None);

        match up.resume(Some(OfflineArg::Load(loaded))) {
            OfflineCoroutineState::Yielded(OfflineYield::WantsFetch { tier, handles, .. }) => {
                assert_eq!(tier, Tier::Full);
                assert_eq!(handles, vec![Handle::from("1")]);
            }
            state => panic!("expected WantsFetch Full, got {state:?}"),
        }
    }

    #[test]
    fn fetched_unknown_handle_is_skipped() {
        // A fetch reply naming a handle with no placement (a lagging or
        // buggy consumer) is ignored rather than upserted or panicking.
        let loaded = Loaded {
            placements: vec![probed("1", None, Level::Probed)],
            checkpoint: None,
        };
        let mut up = OfflineUpgrade::new("inbox", vec![Handle::from("1")], Tier::Meta);
        let _ = up.resume(None);
        let _ = up.resume(Some(OfflineArg::Load(loaded)));

        let items = vec![FetchedItem {
            handle: Handle::from("ghost"),
            link_id: LinkId::from("msg-x"),
            meta: Meta("hdr".into()),
            body: None,
            revision: None,
        }];
        let ops = match up.resume(Some(OfflineArg::Fetch(items))) {
            OfflineCoroutineState::Yielded(OfflineYield::WantsWrite(ops)) => ops,
            state => panic!("expected WantsWrite, got {state:?}"),
        };
        assert!(ops.is_empty(), "nothing to write: {ops:?}");

        match up.resume(Some(OfflineArg::Write)) {
            OfflineCoroutineState::Complete(Ok(report)) => assert_eq!(report.upgraded, 0),
            state => panic!("expected Complete(Ok), got {state:?}"),
        }
    }

    #[test]
    fn full_mixes_dedup_hits_and_fetch_misses() {
        crate::testlog::init();
        // Two placements at meta level: one link id resolves in the object
        // store (linked without a fetch), the other misses and is fetched.
        let loaded = Loaded {
            placements: vec![
                probed("1", Some("msg-a"), Level::Meta),
                probed("2", Some("msg-b"), Level::Meta),
            ],
            checkpoint: None,
        };
        let mut up = OfflineUpgrade::new(
            "inbox",
            vec![Handle::from("1"), Handle::from("2")],
            Tier::Full,
        );
        let _ = up.resume(None);
        let _ = up.resume(Some(OfflineArg::Load(loaded)));

        let mut known = BTreeMap::new();
        known.insert(LinkId::from("msg-a"), Hash::from("h-a"));

        let handles = match up.resume(Some(OfflineArg::LookupObject(known))) {
            OfflineCoroutineState::Yielded(OfflineYield::WantsFetch { handles, .. }) => handles,
            state => panic!("expected WantsFetch for the miss, got {state:?}"),
        };
        assert_eq!(handles, vec![Handle::from("2")], "only the miss fetches");

        let items = vec![FetchedItem {
            handle: Handle::from("2"),
            link_id: LinkId::from("msg-b"),
            meta: Meta("hdr".into()),
            body: Some((Hash::from("h-b"), b"body".to_vec())),
            revision: None,
        }];
        let _ = up.resume(Some(OfflineArg::Fetch(items)));

        let report = match up.resume(Some(OfflineArg::Write)) {
            OfflineCoroutineState::Complete(Ok(report)) => report,
            state => panic!("expected Complete(Ok), got {state:?}"),
        };
        assert_eq!(report.upgraded, 2);
        assert_eq!(report.deduped, 1);
        assert_eq!(report.fetched, 1);
    }
}
