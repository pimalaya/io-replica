//! I/O-free coroutine to raise placements to a higher detail level.
//!
//! A pure pull, never a merge. It loads the targeted placements, then for
//! [`ReplicaTier::Full`] resolves their link ids against the object store
//! first: a body already stored under another collection is linked without
//! any network round-trip. This stores one body for an item that appears in
//! several collections at once, and backs a unified across-collections
//! view.

use core::mem;

use alloc::{collections::BTreeMap, vec::Vec};

use log::{debug, trace};
use thiserror::Error;

use crate::{
    change::ReplicaWriteOp,
    collection::ReplicaCollectionId,
    coroutine::*,
    object::ReplicaObject,
    placement::{ReplicaHandle, ReplicaLevel, ReplicaPlacement},
    remote::{ReplicaFetchedBody, ReplicaTier},
    storage::ReplicaLoadScope,
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
    #[error("Replica UPGRADE failed: unexpected coroutine arg")]
    UnexpectedArg,
    /// The driver resumed without the arg the pending yield required.
    #[error("Replica UPGRADE failed: missing coroutine arg")]
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
    ///
    /// The level is a claim and the payload is the fact, so a row that
    /// reads as high enough while holding nothing is upgraded again: it
    /// would otherwise be skipped forever, since nothing else revisits
    /// what already reads as reached.
    fn pending_handles(&self) -> Vec<ReplicaHandle> {
        self.handles
            .iter()
            .filter(|h| match self.placements.get(h) {
                Some(p) => match self.tier {
                    ReplicaTier::Meta => p.level < ReplicaLevel::Meta || p.meta.is_none(),
                    ReplicaTier::Full => p.level < ReplicaLevel::Full || p.object.is_none(),
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
        match (&self.state, arg) {
            (State::Start, None) => {
                debug!("load target items from storage");
                self.state = State::PendingLoad;
                ReplicaCoroutineState::Yielded(ReplicaYield::WantsLoad {
                    collection: self.collection.clone(),
                    scope: ReplicaLoadScope::Handles(self.handles.clone()),
                })
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
                            .filter(|p| !is_mutable(p))
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
                        .filter(|_| !is_mutable(placement))
                        .and_then(|link| known.get(link).cloned());

                    match hit {
                        Some(hash) => {
                            let mut patched = placement.clone();
                            // NOTE: the base moves with the body, as it does on
                            // the fetch below. A body linked from the store is
                            // the item's synced content, and a base left behind
                            // reads as a local edit on every later sync.
                            if let Some(base) = &mut patched.base {
                                base.object = Some(hash.clone());
                            }
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
                    // NOTE: keep an already-resolved link id. A fetch establishes
                    // the link only for a not-yet-linked item (a `Meta` upgrade of
                    // a probed placement); a `Full` body fetch of a linked item
                    // must not re-identify it. Two tiers can otherwise disagree on
                    // the link (a server ENVELOPE reporting a `Message-ID` the body
                    // parser does not, or a differently formatted date in the
                    // fallback digest), which would strand the linked item and
                    // duplicate it under the body's link.
                    if patched.link_id.is_none() {
                        patched.link_id = Some(item.link_id);
                    }
                    patched.meta = Some(item.meta);
                    // NOTE: unlike the link id, the key is refreshed on
                    // every tier. It is a projection of the content, not
                    // an identity, so the later and better-informed
                    // derivation should win: a `Full` body carries the
                    // real date where an envelope may have carried none.
                    // A connector whose kind defines no key sends an
                    // empty one, which reads as unknown either way.
                    patched.sort_key = item.sort_key;

                    match (self.tier, item.body) {
                        (ReplicaTier::Full, Some(body)) => {
                            // NOTE: the body is either inline bytes to store, or
                            // a reference to an object the consumer already
                            // streamed into its blob store (bounded memory) —
                            // then the store op carries no bytes.
                            let (object, store_body) = match body {
                                ReplicaFetchedBody::Inline { hash, bytes } => {
                                    let object = ReplicaObject {
                                        hash,
                                        size: bytes.len(),
                                    };
                                    (object, Some(bytes))
                                }
                                ReplicaFetchedBody::Persisted { hash, size } => {
                                    (ReplicaObject { hash, size }, None)
                                }
                            };
                            let hash = object.hash.clone();
                            self.ops.push(ReplicaWriteOp::StoreObject {
                                object,
                                body: store_body,
                            });

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

/// Whether the placement's content is mutable, which the last-synced revision
/// is the mark of: only a source that rewrites a body in place has one.
///
/// Such a placement is fetched rather than linked from the store: the link id
/// says two copies are the same item, not that they hold the same bytes, and a
/// revision is what makes the difference observable. Linking one copy's body
/// under another's revision would record a body no fetch ever confirmed.
fn is_mutable(placement: &ReplicaPlacement) -> bool {
    placement
        .base
        .as_ref()
        .is_some_and(|base| base.revision.is_some())
}

enum State {
    Start,
    PendingLoad,
    PendingLookup,
    PendingFetch,
    PendingWrite,
}

#[cfg(test)]
mod tests {
    use alloc::{collections::BTreeMap, string::String, vec};

    use crate::{
        object::ReplicaHash,
        placement::{ReplicaBase, ReplicaFlags, ReplicaLinkId, ReplicaMeta, ReplicaStatus},
        remote::{ReplicaFetchedBody, ReplicaFetchedItem},
        storage::ReplicaLoaded,
        upgrade::*,
    };

    fn probed(handle: &str, link: Option<&str>, level: ReplicaLevel) -> ReplicaPlacement {
        ReplicaPlacement {
            sort_key: Default::default(),
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
            sort_key: Default::default(),
            handle: ReplicaHandle::from("1"),
            link_id: ReplicaLinkId::from("msg-b"),
            meta: ReplicaMeta("hdr".into()),
            body: Some(ReplicaFetchedBody::Inline {
                hash: ReplicaHash::from("h-b"),
                bytes: b"body".to_vec(),
            }),
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
    fn fetch_results_are_matched_by_handle_not_order() {
        // A pooled or reordered fetch may return results in any order; each must
        // land on its own handle, so a concurrent consumer is correct.
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
        let _ = up.resume(Some(ReplicaArg::LookupObject(BTreeMap::new())));

        // Results returned in the reverse of the requested order.
        let items = vec![
            ReplicaFetchedItem {
                sort_key: Default::default(),
                handle: ReplicaHandle::from("2"),
                link_id: ReplicaLinkId::from("msg-b"),
                meta: ReplicaMeta("h".into()),
                body: Some(ReplicaFetchedBody::Inline {
                    hash: ReplicaHash::from("h-b"),
                    bytes: b"bbb".to_vec(),
                }),
                revision: None,
            },
            ReplicaFetchedItem {
                sort_key: Default::default(),
                handle: ReplicaHandle::from("1"),
                link_id: ReplicaLinkId::from("msg-a"),
                meta: ReplicaMeta("h".into()),
                body: Some(ReplicaFetchedBody::Inline {
                    hash: ReplicaHash::from("h-a"),
                    bytes: b"aaaaa".to_vec(),
                }),
                revision: None,
            },
        ];
        let ops = match up.resume(Some(ReplicaArg::Fetch(items))) {
            ReplicaCoroutineState::Yielded(ReplicaYield::WantsWrite(ops)) => ops,
            state => panic!("expected WantsWrite, got {state:?}"),
        };

        // Each handle's placement pins its own object, not the other's.
        let object_for = |handle: &str| {
            ops.iter().find_map(|op| match op {
                ReplicaWriteOp::UpsertPlacement(p) if p.handle.as_str() == handle => {
                    p.object.clone()
                }
                _ => None,
            })
        };
        assert_eq!(object_for("1"), Some(ReplicaHash::from("h-a")));
        assert_eq!(object_for("2"), Some(ReplicaHash::from("h-b")));
    }

    #[test]
    fn a_full_fetch_keeps_an_already_resolved_link_id() {
        // A Full body fetch must not re-identify an already-linked item. If the
        // body parses to a different link than the Meta tier resolved (a server
        // ENVELOPE reporting a Message-ID the body parser misses, say), the
        // placement keeps its original link and simply rises to Full — it is not
        // duplicated under the body's link (which stranded the Meta item and
        // re-fetched it every sync).
        let loaded = ReplicaLoaded {
            placements: vec![probed("1", Some("mid:real"), ReplicaLevel::Meta)],
            checkpoint: None,
        };
        let mut up =
            ReplicaUpgrade::new("inbox", vec![ReplicaHandle::from("1")], ReplicaTier::Full);
        let _ = up.resume(None);
        let _ = up.resume(Some(ReplicaArg::Load(loaded)));
        let _ = up.resume(Some(ReplicaArg::LookupObject(BTreeMap::new())));

        let items = vec![ReplicaFetchedItem {
            sort_key: Default::default(),
            handle: ReplicaHandle::from("1"),
            // The body parses to a *different* link than the Meta tier resolved.
            link_id: ReplicaLinkId::from("alt:divergent"),
            meta: ReplicaMeta("hdr".into()),
            body: Some(ReplicaFetchedBody::Persisted {
                hash: ReplicaHash::from("h"),
                size: 10,
            }),
            revision: None,
        }];
        let ops = match up.resume(Some(ReplicaArg::Fetch(items))) {
            ReplicaCoroutineState::Yielded(ReplicaYield::WantsWrite(ops)) => ops,
            state => panic!("expected WantsWrite, got {state:?}"),
        };
        let placement = ops
            .iter()
            .find_map(|op| match op {
                ReplicaWriteOp::UpsertPlacement(p) => Some(p),
                _ => None,
            })
            .expect("a placement upsert");
        assert_eq!(
            placement.link_id,
            Some(ReplicaLinkId::from("mid:real")),
            "the Full fetch keeps the Meta-resolved link, not the body's"
        );
        assert_eq!(placement.level, ReplicaLevel::Full);
    }

    #[test]
    fn a_meta_fetch_still_sets_the_link_of_an_unlinked_item() {
        // The complement: a not-yet-linked (probed) item DOES take the fetched
        // link — that is how the Meta tier establishes identity.
        let loaded = ReplicaLoaded {
            placements: vec![probed("1", None, ReplicaLevel::Probed)],
            checkpoint: None,
        };
        let mut up =
            ReplicaUpgrade::new("inbox", vec![ReplicaHandle::from("1")], ReplicaTier::Meta);
        let _ = up.resume(None);
        let _ = up.resume(Some(ReplicaArg::Load(loaded)));
        let _ = up.resume(Some(ReplicaArg::LookupObject(BTreeMap::new())));

        let items = vec![ReplicaFetchedItem {
            sort_key: Default::default(),
            handle: ReplicaHandle::from("1"),
            link_id: ReplicaLinkId::from("mid:resolved"),
            meta: ReplicaMeta("hdr".into()),
            body: None,
            revision: None,
        }];
        let ops = match up.resume(Some(ReplicaArg::Fetch(items))) {
            ReplicaCoroutineState::Yielded(ReplicaYield::WantsWrite(ops)) => ops,
            state => panic!("expected WantsWrite, got {state:?}"),
        };
        let placement = ops
            .iter()
            .find_map(|op| match op {
                ReplicaWriteOp::UpsertPlacement(p) => Some(p),
                _ => None,
            })
            .expect("a placement upsert");
        assert_eq!(
            placement.link_id,
            Some(ReplicaLinkId::from("mid:resolved")),
            "a probed item takes the fetched link"
        );
    }

    #[test]
    fn a_persisted_body_stores_the_object_without_bytes() {
        // A consumer that streamed the body straight into its blob store reports
        // it by (hash, size); the engine records the object with no bytes.
        let loaded = ReplicaLoaded {
            placements: vec![probed("1", Some("msg-b"), ReplicaLevel::Meta)],
            checkpoint: None,
        };
        let mut up =
            ReplicaUpgrade::new("inbox", vec![ReplicaHandle::from("1")], ReplicaTier::Full);
        let _ = up.resume(None);
        let _ = up.resume(Some(ReplicaArg::Load(loaded)));
        let _ = up.resume(Some(ReplicaArg::LookupObject(BTreeMap::new())));

        let items = vec![ReplicaFetchedItem {
            sort_key: Default::default(),
            handle: ReplicaHandle::from("1"),
            link_id: ReplicaLinkId::from("msg-b"),
            meta: ReplicaMeta("hdr".into()),
            body: Some(ReplicaFetchedBody::Persisted {
                hash: ReplicaHash::from("h-b"),
                size: 4096,
            }),
            revision: None,
        }];
        let ops = match up.resume(Some(ReplicaArg::Fetch(items))) {
            ReplicaCoroutineState::Yielded(ReplicaYield::WantsWrite(ops)) => ops,
            state => panic!("expected WantsWrite, got {state:?}"),
        };
        match &ops[0] {
            ReplicaWriteOp::StoreObject { object, body } => {
                assert_eq!(object.hash, ReplicaHash::from("h-b"));
                assert_eq!(object.size, 4096, "size comes from the report, not bytes");
                assert!(body.is_none(), "no bytes: the fetch already persisted them");
            }
            other => panic!("expected StoreObject, got {other:?}"),
        }
        // The placement still pins the object and rises to Full.
        assert!(matches!(
            &ops[1],
            ReplicaWriteOp::UpsertPlacement(p)
                if p.object == Some(ReplicaHash::from("h-b")) && p.level == ReplicaLevel::Full
        ));
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
            sort_key: Default::default(),
            handle: ReplicaHandle::from("1"),
            link_id: ReplicaLinkId::from("msg-b"),
            meta: ReplicaMeta("hdr".into()),
            body: Some(ReplicaFetchedBody::Inline {
                hash: ReplicaHash::from("h-b"),
                bytes: b"body".to_vec(),
            }),
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
        let mut placement = probed("1", Some("x"), ReplicaLevel::Full);
        placement.object = Some(ReplicaHash::from("h1"));
        let loaded = ReplicaLoaded {
            placements: vec![placement],
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
    fn a_full_row_holding_no_body_is_upgraded_again() {
        // The level is a claim, the object is the fact. A row recorded at
        // Full with no body would otherwise be skipped forever, since
        // nothing revisits what already reads as Full.
        let loaded = ReplicaLoaded {
            placements: vec![probed("1", None, ReplicaLevel::Full)],
            checkpoint: None,
        };
        let mut up =
            ReplicaUpgrade::new("inbox", vec![ReplicaHandle::from("1")], ReplicaTier::Full);
        let _ = up.resume(None);

        match up.resume(Some(ReplicaArg::Load(loaded))) {
            ReplicaCoroutineState::Yielded(ReplicaYield::WantsFetch { handles, .. }) => {
                assert_eq!(handles, vec![ReplicaHandle::from("1")]);
            }
            state => panic!("expected WantsFetch, got {state:?}"),
        }
    }

    #[test]
    fn a_meta_row_holding_no_summary_is_upgraded_again() {
        let loaded = ReplicaLoaded {
            placements: vec![probed("1", None, ReplicaLevel::Meta)],
            checkpoint: None,
        };
        let mut up =
            ReplicaUpgrade::new("inbox", vec![ReplicaHandle::from("1")], ReplicaTier::Meta);
        let _ = up.resume(None);

        match up.resume(Some(ReplicaArg::Load(loaded))) {
            ReplicaCoroutineState::Yielded(ReplicaYield::WantsFetch { handles, .. }) => {
                assert_eq!(handles, vec![ReplicaHandle::from("1")]);
            }
            state => panic!("expected WantsFetch, got {state:?}"),
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
            sort_key: Default::default(),
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
            sort_key: Default::default(),
            handle: ReplicaHandle::from("2"),
            link_id: ReplicaLinkId::from("msg-b"),
            meta: ReplicaMeta("hdr".into()),
            body: Some(ReplicaFetchedBody::Inline {
                hash: ReplicaHash::from("h-b"),
                bytes: b"body".to_vec(),
            }),
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

    /// A placement already reconciled once: based, summarised, and with the
    /// revision its source reports (`None` for immutable content).
    fn based(handle: &str, link: &str, revision: Option<&str>) -> ReplicaPlacement {
        let mut placement = probed(handle, Some(link), ReplicaLevel::Meta);
        placement.base = Some(ReplicaBase {
            flags: ReplicaFlags::default(),
            revision: revision.map(String::from),
            object: None,
        });
        placement
    }

    /// Runs a `Full` upgrade of one placement up to its first yield after the
    /// link lookup, answering the lookup with `known`.
    fn upgrade_with_lookup(
        placement: ReplicaPlacement,
        known: BTreeMap<ReplicaLinkId, ReplicaHash>,
    ) -> ReplicaCoroutineState<ReplicaYield, Result<ReplicaUpgradeReport, ReplicaUpgradeError>>
    {
        let handle = placement.handle.clone();
        let loaded = ReplicaLoaded {
            placements: vec![placement],
            checkpoint: None,
        };
        let mut up = ReplicaUpgrade::new("inbox", vec![handle], ReplicaTier::Full);
        let _ = up.resume(None);

        match up.resume(Some(ReplicaArg::Load(loaded))) {
            ReplicaCoroutineState::Yielded(ReplicaYield::WantsLookupObject(_)) => {
                up.resume(Some(ReplicaArg::LookupObject(known)))
            }
            state => state,
        }
    }

    #[test]
    fn a_deduped_body_rebases_so_the_placement_reads_clean() {
        // The same message in two collections: the second placement links the
        // body the first one fetched. Leaving its base behind reads as a local
        // edit, which a storage projects dirty and re-derives on every sync,
        // for good.
        let known = BTreeMap::from([(ReplicaLinkId::from("msg-a"), ReplicaHash::from("h-a"))]);

        let ops = match upgrade_with_lookup(based("2", "msg-a", None), known) {
            ReplicaCoroutineState::Yielded(ReplicaYield::WantsWrite(ops)) => ops,
            state => panic!("expected WantsWrite (no fetch), got {state:?}"),
        };
        let ReplicaWriteOp::UpsertPlacement(placement) = &ops[0] else {
            panic!("expected UpsertPlacement, got {:?}", ops[0]);
        };

        assert_eq!(placement.object, Some(ReplicaHash::from("h-a")));
        assert_eq!(placement.level, ReplicaLevel::Full);
        assert_eq!(
            placement.base.as_ref().and_then(|base| base.object.clone()),
            Some(ReplicaHash::from("h-a")),
            "the base holds the linked body, so nothing reads as edited"
        );
    }

    #[test]
    fn a_mutable_placement_is_fetched_rather_than_linked() {
        // A link id says two copies are the same item, not that they hold the
        // same bytes. Where a revision makes the difference observable, the
        // body is fetched, so no copy's bytes are recorded under another's
        // revision.
        let known = BTreeMap::from([(ReplicaLinkId::from("uid:card-1"), ReplicaHash::from("h-a"))]);

        let state = upgrade_with_lookup(based("card-1.vcf", "uid:card-1", Some("etag-1")), known);

        match state {
            ReplicaCoroutineState::Yielded(ReplicaYield::WantsFetch { handles, tier, .. }) => {
                assert_eq!(tier, ReplicaTier::Full);
                assert_eq!(handles, vec![ReplicaHandle::from("card-1.vcf")]);
            }
            state => panic!("expected WantsFetch, got {state:?}"),
        }
    }
}
