//! I/O-free coroutine to raise placements to a higher detail level.
//!
//! A pure pull, never a merge. It loads the targeted placements, then
//! for [`ReplicaTier::Full`] resolves their link ids against the object
//! store first: a body already stored under another collection is linked
//! with no network round-trip. One body is therefore stored for an item
//! appearing in several collections, which is what backs the unified
//! across-collections view.
//!
//! It is also where identity is settled, since a fetch is what reads
//! one. A collection cannot hold two placements under one link id, so a
//! second copy of an identity is linked under a key minted from it
//! rather than left unlinked: a source holding two resources holds two
//! items, whatever its protocol says about uniqueness.

use core::mem;

use alloc::{collections::BTreeMap, string::String, vec::Vec};

use log::{debug, trace};

use crate::{
    change::ReplicaWriteOp,
    collection::ReplicaCollectionId,
    coroutine::*,
    object::ReplicaObject,
    placement::{ReplicaHandle, ReplicaLevel, ReplicaLinkId, ReplicaPlacement, ReplicaStatus},
    remote::{ReplicaFetchedBody, ReplicaFetchedItem, ReplicaTier},
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

/// I/O-free UPGRADE coroutine.
pub struct ReplicaUpgrade {
    collection: ReplicaCollectionId,
    handles: Vec<ReplicaHandle>,
    tier: ReplicaTier,
    placements: BTreeMap<ReplicaHandle, ReplicaPlacement>,
    /// Fetched items held between the fetch and the identity check.
    fetched: Vec<ReplicaFetchedItem>,
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
            fetched: Vec::new(),
            ops: Vec::new(),
            report: ReplicaUpgradeReport::default(),
            state: State::Start,
        }
    }

    /// Requested handles that still need work for the target tier.
    ///
    /// The level is a claim and the payload is the fact, so a row
    /// reading as high enough while holding nothing is upgraded again;
    /// nothing else revisits what already reads as reached.
    ///
    /// A conflicted placement is asked a different question: its own
    /// body is the local side of a divergence, which no fetch supplies,
    /// so what it still needs is the body the remote holds instead.
    fn pending_handles(&self) -> Vec<ReplicaHandle> {
        self.handles
            .iter()
            .filter(|h| match self.placements.get(h) {
                Some(p) => match self.tier {
                    ReplicaTier::Meta => p.level < ReplicaLevel::Meta || p.meta.is_none(),
                    ReplicaTier::Full => match is_conflicted(p) {
                        true => p.conflict_object.is_none(),
                        false => p.level < ReplicaLevel::Full || p.object.is_none(),
                    },
                },
                None => false,
            })
            .cloned()
            .collect()
    }
}

impl ReplicaCoroutine for ReplicaUpgrade {
    type Yield = ReplicaYield;
    type Return = Result<ReplicaUpgradeReport, ReplicaArgError>;

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
                    self.state = State::Done;
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
                            .filter(|p| !is_mutable(p) && !is_conflicted(p))
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
                        .filter(|_| !is_mutable(placement) && !is_conflicted(placement))
                        .and_then(|link| known.get(link).cloned());

                    match hit {
                        Some(hash) => {
                            let mut patched = placement.clone();
                            // NOTE: the base moves with the body, as on
                            // the fetch below: a body linked from the
                            // store is the item's synced content, and a
                            // base left behind reads as a local edit on
                            // every later sync.
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

                // NOTE: an identity this fetch would newly establish is
                // checked against every placement that could already
                // hold it, not just the ones being upgraded: a batch
                // hydrating only the second copy would otherwise link it
                // and destroy the evidence.
                let fresh: Vec<ReplicaLinkId> = items
                    .iter()
                    .filter(|item| {
                        self.placements
                            .get(&item.handle)
                            .is_some_and(|p| p.link_id.is_none())
                    })
                    .map(|item| item.link_id.clone())
                    .collect();

                self.fetched = items;
                if fresh.is_empty() {
                    return self.write_fetched();
                }

                debug!(
                    "check {} fresh link ids against the collection",
                    fresh.len()
                );
                self.state = State::PendingLinkCheck;
                ReplicaCoroutineState::Yielded(ReplicaYield::WantsLoad {
                    collection: self.collection.clone(),
                    scope: ReplicaLoadScope::Links(fresh),
                })
            }

            (State::PendingLinkCheck, Some(ReplicaArg::Load(loaded))) => {
                // NOTE: holders the upgrade batch never named, folded in
                // so the identity check below sees the whole picture.
                for placement in loaded.placements {
                    self.placements
                        .entry(placement.handle.clone())
                        .or_insert(placement);
                }
                self.write_fetched()
            }

            (State::PendingWrite, Some(ReplicaArg::Write)) => {
                debug!(
                    "upgraded {} items ({} fetched, {} linked from store)",
                    self.report.upgraded, self.report.fetched, self.report.deduped,
                );
                // NOTE: a completed coroutine stays completed: resuming
                // one is a driver bug, not an empty success.
                self.state = State::Done;
                ReplicaCoroutineState::Complete(Ok(self.report))
            }

            (_, Some(_)) => ReplicaCoroutineState::Complete(Err(ReplicaArgError::UnexpectedArg)),
            (_, None) => ReplicaCoroutineState::Complete(Err(ReplicaArgError::MissingArg)),
        }
    }
}

impl ReplicaUpgrade {
    /// Applies the fetched items and yields the write batch.
    fn write_fetched(
        &mut self,
    ) -> ReplicaCoroutineState<ReplicaYield, <Self as ReplicaCoroutine>::Return> {
        // NOTE: who holds each identity, seeded from the collection and
        // extended as this batch resolves more. Both copies of a
        // duplicate are commonly fetched together and neither is linked
        // yet, so a check against the stored rows alone would link both.
        let mut claimed: BTreeMap<ReplicaLinkId, ReplicaHandle> = self
            .placements
            .values()
            .filter_map(|p| Some((p.link_id.clone()?, p.handle.clone())))
            .collect();

        // NOTE: which copy of a duplicate keeps the bare hint decides
        // what the other one is minted as, and a fetch batch carries no
        // order: a consumer servicing it across a connection pool
        // reports in whatever order the pool finished. Claiming in
        // handle order is what makes the mint the same key on a store
        // rebuilt from scratch.
        let mut fetched = mem::take(&mut self.fetched);
        fetched.sort_by(|a, b| a.handle.cmp(&b.handle));

        for item in fetched {
            let Some(placement) = self.placements.get(&item.handle) else {
                continue;
            };
            let mut patched = placement.clone();

            if self.tier == ReplicaTier::Full && is_conflicted(&patched) {
                // NOTE: the fetch answers what the remote holds instead
                // of the local body, which is another question about the
                // same handle: everything but the conflict object
                // describes the local side, and stays as it is.
                let Some(body) = item.body else {
                    continue;
                };
                let (object, bytes) = stored_body(body);
                let hash = object.hash.clone();

                self.ops.push(ReplicaWriteOp::StoreObject {
                    object,
                    body: bytes,
                });
                patched.conflict_object = Some(hash);
                self.ops.push(ReplicaWriteOp::UpsertPlacement(patched));
                self.report.fetched += 1;
                self.report.upgraded += 1;
                continue;
            }

            // NOTE: a fetch establishes the link only for a not-yet-
            // linked item, never re-identifying a linked one. Two tiers
            // can disagree on the link, an ENVELOPE reporting a
            // `Message-ID` the body parser does not, which would strand
            // the linked item and duplicate it under the body's link.
            if patched.link_id.is_none() {
                // NOTE: nor a link another placement already holds:
                // linking the second copy overwrites the first binding's
                // handle, destroying the evidence that the source holds
                // the identity twice. The second copy takes a minted key
                // instead, which makes it an item like any other.
                let link_id = match claimed.contains_key(&item.link_id) {
                    true => mint(&item.link_id, &item.handle),
                    false => item.link_id.clone(),
                };
                claimed.insert(link_id.clone(), item.handle.clone());
                patched.link_id = Some(link_id);
            }
            patched.meta = Some(item.meta);
            // NOTE: unlike the link id, the key is refreshed on every
            // tier: it is a projection of the content, not an identity,
            // so the better-informed derivation wins.
            patched.sort_key = item.sort_key;

            match (self.tier, item.body) {
                (ReplicaTier::Full, Some(body)) => {
                    // NOTE: a body already streamed into the consumer's
                    // blob store carries no bytes in the store op.
                    let (object, bytes) = stored_body(body);
                    let hash = object.hash.clone();
                    self.ops.push(ReplicaWriteOp::StoreObject {
                        object,
                        body: bytes,
                    });

                    // NOTE: the stored object is the remote content as
                    // of the fetch, so the base records both it and the
                    // revision.
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
}

/// Mints the link id of a second copy: `dup:`, the identity hint the
/// fetch resolved, `#`, and the placement's own handle verbatim.
///
/// The form is fixed by the store format (pimdir SPEC §9) and
/// three implementations have to agree on it, so it is spelled out here
/// rather than composed. No digest: this crate hashes nothing, the key
/// is opaque and never parsed back, and keeping the handle in it makes
/// the copy traceable to the resource it came from.
///
/// Deriving it from the hint and the handle alone is what makes it
/// deterministic: the same collection read again from an empty store
/// mints the same key, so a rebuilt replica converges on the rows it had
/// rather than on a second set of duplicates.
fn mint(hint: &ReplicaLinkId, handle: &ReplicaHandle) -> ReplicaLinkId {
    let mut key = String::from("dup:");
    key.push_str(hint.as_str());
    key.push('#');
    key.push_str(handle.as_str());
    ReplicaLinkId(key)
}

/// Splits a fetched body into the object to record and the bytes to
/// store, which a consumer that streamed the body into its own blob
/// store has already written.
fn stored_body(body: ReplicaFetchedBody) -> (ReplicaObject, Option<Vec<u8>>) {
    match body {
        ReplicaFetchedBody::Inline { hash, bytes } => {
            let object = ReplicaObject {
                hash,
                size: bytes.len(),
            };

            (object, Some(bytes))
        }
        ReplicaFetchedBody::Persisted { hash, size } => (ReplicaObject { hash, size }, None),
    }
}

/// Whether the placement holds the local side of a divergence, so a
/// fetch of it answers what the remote holds instead rather than what
/// the placement itself holds.
///
/// Such a placement is fetched rather than linked from the store, for
/// the reason a mutable one is: a link id says two copies are the same
/// item, and the conflict is about bytes the remote alone has.
fn is_conflicted(placement: &ReplicaPlacement) -> bool {
    placement.status == ReplicaStatus::Conflict
}

/// Whether the placement's content is mutable, which the last-synced
/// revision is the mark of: only a source rewriting a body in place has
/// one.
///
/// Such a placement is fetched rather than linked from the store: the
/// link id says two copies are the same item, not that they hold the
/// same bytes, so linking one copy's body under another's revision would
/// record a body no fetch confirmed.
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
    PendingLinkCheck,
    PendingWrite,
    Done,
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
            conflict_object: None,
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
        // a pooled or reordered fetch returns results in any order, and
        // each must land on its own handle
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

        // results returned in the reverse of the requested order
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

        // each handle's placement pins its own object
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
        // when the body parses to a different link than the Meta tier
        // resolved, the placement keeps its original link and rises to
        // Full rather than being duplicated under the body's link
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
            // a different link than the Meta tier resolved
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
        // the complement: a probed item does take the fetched link,
        // which is how the Meta tier establishes identity
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
        // a fetch establishing a fresh identity checks it against the
        // collection first, and nothing else holds it here
        match up.resume(Some(ReplicaArg::Fetch(items))) {
            ReplicaCoroutineState::Yielded(ReplicaYield::WantsLoad {
                scope: ReplicaLoadScope::Links(links),
                ..
            }) => assert_eq!(links, vec![ReplicaLinkId::from("mid:resolved")]),
            state => panic!("expected WantsLoad, got {state:?}"),
        }
        let ops = match up.resume(Some(ReplicaArg::Load(ReplicaLoaded::default()))) {
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

    /// The link ids the upgrade wrote, by handle.
    fn links(ops: &[ReplicaWriteOp]) -> BTreeMap<&str, Option<&str>> {
        ops.iter()
            .filter_map(|op| match op {
                ReplicaWriteOp::UpsertPlacement(p) => {
                    Some((p.handle.as_str(), p.link_id.as_ref().map(|l| l.as_str())))
                }
                _ => None,
            })
            .collect()
    }

    /// A meta upgrade of `handles`, fetched as all resolving to `link`,
    /// checked against `stored` and returning the write batch.
    fn upgrade_twins(
        handles: &[&str],
        link: &str,
        loaded: Vec<ReplicaPlacement>,
        stored: Vec<ReplicaPlacement>,
    ) -> Vec<ReplicaWriteOp> {
        crate::testlog::init();
        let requested = handles.iter().copied().map(ReplicaHandle::from).collect();
        let mut up = ReplicaUpgrade::new("inbox", requested, ReplicaTier::Meta);
        let _ = up.resume(None);
        let _ = up.resume(Some(ReplicaArg::Load(ReplicaLoaded {
            placements: loaded,
            checkpoint: None,
        })));

        let items = handles
            .iter()
            .map(|handle| ReplicaFetchedItem {
                sort_key: Default::default(),
                handle: ReplicaHandle::from(*handle),
                link_id: ReplicaLinkId::from(link),
                meta: ReplicaMeta("hdr".into()),
                body: None,
                revision: None,
            })
            .collect();

        // an already-linked placement resolves no fresh identity, so the
        // upgrade writes without checking the collection at all
        let ops = match up.resume(Some(ReplicaArg::Fetch(items))) {
            ReplicaCoroutineState::Yielded(ReplicaYield::WantsWrite(ops)) => Some(ops),
            ReplicaCoroutineState::Yielded(ReplicaYield::WantsLoad {
                scope: ReplicaLoadScope::Links(_),
                ..
            }) => None,
            state => panic!("expected WantsWrite or a link check, got {state:?}"),
        };

        match ops {
            Some(ops) => ops,
            None => match up.resume(Some(ReplicaArg::Load(ReplicaLoaded {
                placements: stored,
                checkpoint: None,
            }))) {
                ReplicaCoroutineState::Yielded(ReplicaYield::WantsWrite(ops)) => ops,
                state => panic!("expected WantsWrite, got {state:?}"),
            },
        }
    }

    #[test]
    fn a_second_copy_of_one_identity_is_minted() {
        // both copies are hydrated by one batch and neither is linked
        // yet, so the claim is tracked as the batch resolves it
        let ops = upgrade_twins(
            &["u1", "u2"],
            "m1",
            vec![
                probed("u1", None, ReplicaLevel::Probed),
                probed("u2", None, ReplicaLevel::Probed),
            ],
            Vec::new(),
        );

        assert_eq!(
            links(&ops),
            BTreeMap::from([("u1", Some("m1")), ("u2", Some("dup:m1#u2"))]),
            "the first copy keeps the hint, the second is minted from it \
             and its own handle",
        );
    }

    #[test]
    fn the_mint_is_decided_against_the_collection_not_the_batch() {
        // only the second copy is hydrated here, so the holder is known
        // from the link check alone
        let ops = upgrade_twins(
            &["u2"],
            "m1",
            vec![probed("u2", None, ReplicaLevel::Probed)],
            vec![probed("u1", Some("m1"), ReplicaLevel::Meta)],
        );

        assert_eq!(
            links(&ops),
            BTreeMap::from([("u2", Some("dup:m1#u2"))]),
            "a batch that never names the holder still mints",
        );
    }

    #[test]
    fn a_minted_copy_is_not_minted_again() {
        // the same handle re-fetched keeps the key it was given: a fetch
        // establishes a link only for a placement that has none, so the
        // mint happens once and the copy keeps its identity for good
        let ops = upgrade_twins(
            &["u2"],
            "m1",
            vec![probed("u2", Some("dup:m1#u2"), ReplicaLevel::Probed)],
            vec![probed("u1", Some("m1"), ReplicaLevel::Meta)],
        );

        assert_eq!(
            links(&ops),
            BTreeMap::from([("u2", Some("dup:m1#u2"))]),
            "no dup:dup:m1#u2#u2",
        );
    }

    #[test]
    fn a_persisted_body_stores_the_object_without_bytes() {
        // a consumer that streamed the body into its blob store reports
        // it by (hash, size), and the object is recorded with no bytes
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
        // the placement still pins the object and rises to Full
        assert!(matches!(
            &ops[1],
            ReplicaWriteOp::UpsertPlacement(p)
                if p.object == Some(ReplicaHash::from("h-b")) && p.level == ReplicaLevel::Full
        ));
    }

    #[test]
    fn full_fetch_stamps_the_base_revision_and_object() {
        // a fetched body is the remote content as of the fetch, so the
        // base records the revision and pins the stored body
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
        // the level is a claim, the object the fact: a row recorded at
        // Full with no body would otherwise be skipped forever
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
            ReplicaCoroutineState::Complete(Err(ReplicaArgError::MissingArg)) => {}
            state => panic!("expected MissingArg, got {state:?}"),
        }
    }

    /// An empty report is indistinguishable from a run that did nothing,
    /// so a driver resuming a finished coroutine must be told.
    #[test]
    fn a_completed_upgrade_does_not_resume() {
        let mut placement = probed("1", Some("x"), ReplicaLevel::Full);
        placement.object = Some(ReplicaHash::from("h1"));
        let loaded = ReplicaLoaded {
            placements: vec![placement],
            checkpoint: None,
        };
        let mut up =
            ReplicaUpgrade::new("inbox", vec![ReplicaHandle::from("1")], ReplicaTier::Full);
        let _ = up.resume(None);
        let _ = up.resume(Some(ReplicaArg::Load(loaded)));

        match up.resume(Some(ReplicaArg::Write)) {
            ReplicaCoroutineState::Complete(Err(ReplicaArgError::UnexpectedArg)) => {}
            state => panic!("expected UnexpectedArg, got {state:?}"),
        }
    }

    #[test]
    fn unexpected_arg_errors() {
        let mut up =
            ReplicaUpgrade::new("inbox", vec![ReplicaHandle::from("1")], ReplicaTier::Meta);
        let _ = up.resume(None);
        match up.resume(Some(ReplicaArg::Write)) {
            ReplicaCoroutineState::Complete(Err(ReplicaArgError::UnexpectedArg)) => {}
            state => panic!("expected UnexpectedArg, got {state:?}"),
        }
    }

    #[test]
    fn unknown_handle_completes_without_work() {
        // a requested handle with no placement is skipped, not invented
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
        // probed placements carry no link id yet, so there is nothing to
        // look up and the full upgrade fetches directly
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
        // a fetch reply naming a handle with no placement is ignored
        // rather than upserted
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
        // one link id resolves in the object store and is linked without
        // a fetch, the other misses and is fetched
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

    /// A placement already reconciled once: based, summarised, and
    /// carrying the revision its source reports.
    fn based(handle: &str, link: &str, revision: Option<&str>) -> ReplicaPlacement {
        let mut placement = probed(handle, Some(link), ReplicaLevel::Meta);
        placement.base = Some(ReplicaBase {
            flags: ReplicaFlags::default(),
            revision: revision.map(String::from),
            object: None,
        });
        placement
    }

    /// Runs a `Full` upgrade of one placement up to its first yield
    /// after the link lookup, answering it with `known`.
    fn upgrade_with_lookup(
        placement: ReplicaPlacement,
        known: BTreeMap<ReplicaLinkId, ReplicaHash>,
    ) -> ReplicaCoroutineState<ReplicaYield, Result<ReplicaUpgradeReport, ReplicaArgError>> {
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
        // the second placement links the body the first one fetched;
        // leaving its base behind would read as a local edit, which a
        // storage projects dirty and re-derives on every sync
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
        // a link id says two copies are the same item, not that they
        // hold the same bytes, so where a revision makes the difference
        // observable the body is fetched
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

    /// A conflicted placement: a based, mutable card holding the local
    /// side of a divergence and the revision the remote holds instead.
    fn conflicted(conflict_object: Option<&str>) -> ReplicaPlacement {
        let mut placement = based("card-1.vcf", "uid:card-1", Some("etag-1"));
        placement.object = Some(ReplicaHash::from("h-local"));
        placement.level = ReplicaLevel::Full;
        placement.status = ReplicaStatus::Conflict;
        placement.conflict_revision = Some(String::from("etag-2"));
        placement.conflict_object = conflict_object.map(ReplicaHash::from);
        placement
    }

    /// Runs a `Full` upgrade of one placement up to its first yield
    /// after the load.
    fn upgrade_full(
        placement: ReplicaPlacement,
    ) -> (
        ReplicaUpgrade,
        ReplicaCoroutineState<ReplicaYield, Result<ReplicaUpgradeReport, ReplicaArgError>>,
    ) {
        let handle = placement.handle.clone();
        let loaded = ReplicaLoaded {
            placements: vec![placement],
            checkpoint: None,
        };
        let mut up = ReplicaUpgrade::new("inbox", vec![handle], ReplicaTier::Full);
        let _ = up.resume(None);
        let state = up.resume(Some(ReplicaArg::Load(loaded)));

        (up, state)
    }

    #[test]
    fn a_conflicted_placement_asks_for_the_diverging_body() {
        // it reads as Full and holds a body, so the level rule would
        // skip it; what it is missing is the other body, the one the
        // remote holds instead, and only a fetch supplies that
        let (_up, state) = upgrade_full(conflicted(None));

        match state {
            ReplicaCoroutineState::Yielded(ReplicaYield::WantsFetch { handles, tier, .. }) => {
                assert_eq!(tier, ReplicaTier::Full);
                assert_eq!(handles, vec![ReplicaHandle::from("card-1.vcf")]);
            }
            state => panic!("expected WantsFetch, got {state:?}"),
        }

        // and once it holds one, the question is answered
        let (_up, state) = upgrade_full(conflicted(Some("h-remote")));

        match state {
            ReplicaCoroutineState::Complete(Ok(report)) => assert_eq!(report.fetched, 0),
            state => panic!("expected Complete(Ok), got {state:?}"),
        }
    }

    #[test]
    fn a_fetched_body_lands_as_the_conflict_object() {
        // the placement's object is what the local side holds and the
        // conflict object what the remote holds instead: a fetch answers
        // the second question, and reading it as the first would drop
        // the local edit the conflict exists to protect
        let (mut up, _state) = upgrade_full(conflicted(None));

        let items = vec![ReplicaFetchedItem {
            handle: ReplicaHandle::from("card-1.vcf"),
            link_id: ReplicaLinkId::from("uid:card-1"),
            meta: ReplicaMeta(String::from("remote")),
            sort_key: Default::default(),
            body: Some(ReplicaFetchedBody::Inline {
                hash: ReplicaHash::from("h-remote"),
                bytes: b"remote".to_vec(),
            }),
            revision: Some(String::from("etag-2")),
        }];

        let ops = match up.resume(Some(ReplicaArg::Fetch(items))) {
            ReplicaCoroutineState::Yielded(ReplicaYield::WantsWrite(ops)) => ops,
            state => panic!("expected WantsWrite, got {state:?}"),
        };
        let ReplicaWriteOp::UpsertPlacement(placement) = &ops[1] else {
            panic!("expected UpsertPlacement, got {:?}", ops[1]);
        };

        assert_eq!(
            placement.conflict_object,
            Some(ReplicaHash::from("h-remote"))
        );
        assert_eq!(
            placement.object,
            Some(ReplicaHash::from("h-local")),
            "the local side of the divergence is untouched"
        );
        assert_eq!(
            placement
                .base
                .as_ref()
                .and_then(|base| base.revision.clone()),
            Some(String::from("etag-1")),
            "nor does the fetch rebase what it never merged"
        );
    }
}
