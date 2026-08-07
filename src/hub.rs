//! Multi-source hub: shared content with a base per source.
//!
//! One logical item can live on several sources (a `left` and a `right`
//! server, a server and a phone). The hub holds that item's current content
//! once, plus a last-synced base per source, so a storage that wraps it can
//! [`project`] a per-source placement and [`absorb`] the engine's writes
//! back. Because a projected placement carries the shared content against the
//! source's own base, a change another source folded into the hub reads as
//! locally dirty for this source and the engine's ordinary reconcile pushes
//! it: propagation falls out of the per-source merge, with no cross-merge and
//! no merge-core change. The hub propagates adds, flags, deletes, and resolves
//! cross-source content conflicts by policy.
//!
//! [`project`]: ReplicaHub::project
//! [`absorb`]: ReplicaHub::absorb

use alloc::{collections::BTreeMap, string::String, vec::Vec};

use crate::{
    change::ReplicaWriteOp,
    collection::ReplicaCollectionId,
    object::ReplicaHash,
    placement::{
        ReplicaBase, ReplicaFlags, ReplicaHandle, ReplicaLevel, ReplicaLinkId, ReplicaMeta,
        ReplicaPlacement, ReplicaStatus,
    },
};

/// A source of a shared item: one authoritative replica (`left`, `right`,
/// `phone`).
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct ReplicaSourceId(pub String);

impl ReplicaSourceId {
    /// Borrows the id as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<&str> for ReplicaSourceId {
    fn from(value: &str) -> Self {
        Self(value.into())
    }
}

impl From<String> for ReplicaSourceId {
    fn from(value: String) -> Self {
        Self(value)
    }
}

/// One source's binding of a shared item: its handle there, the base last
/// synced with it, and whether that source's own sync is stuck on a conflict.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReplicaSourceBinding {
    /// The item's handle on this source.
    pub handle: ReplicaHandle,
    /// The last state synced with this source; `None` until first reconciled.
    pub base: Option<ReplicaBase>,
    /// Set when *this source* and its own remote diverged and the merge left
    /// the placement [`ReplicaStatus::Conflict`].
    ///
    /// Distinct from [`ReplicaHubItem::conflicted`], the **cross-source**
    /// conflict (two sources edited the shared body differently). A two-source
    /// store needs both, independently: one says "left and its server
    /// disagree", the other "left and right disagree". Cleared by an upsert of
    /// any other status, so a consumer resolving the conflict with an edit
    /// needs no explicit resolution call.
    pub conflicted: bool,
    /// The remote revision observed when this binding was marked conflicted —
    /// what a resolver fetches and merges against. `None` when not conflicted,
    /// or when the remote reports no revision.
    pub conflict_revision: Option<String>,
}

/// How the hub resolves a cross-source content conflict — both sources edited
/// the same item's body to different values since they last agreed.
///
/// Flags never reach this: they merge element-wise. Only mutable-content
/// backends conflict; immutable ones mint a new link id per body.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum ReplicaHubConflict {
    /// Flag the item conflicted and record the diverging body for the consumer
    /// to resolve; keep the already-shared body, so nothing is lost.
    #[default]
    Manual,
    /// Last-writer-wins: adopt the incoming body, overwriting the shared one.
    PreferIncoming,
    /// Keep the already-shared body, dropping the incoming one.
    PreferExisting,
}

/// A logical item shared across sources: its current content plus a binding per
/// source that holds it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReplicaHubItem {
    /// The current flag set, shared by every source.
    pub flags: ReplicaFlags,
    /// The current body, shared by every source; `None` until hydrated.
    pub object: Option<ReplicaHash>,
    /// The current summary, shared by every source; `None` until fetched.
    pub meta: Option<ReplicaMeta>,
    /// The highest detail level any source has reached.
    pub level: ReplicaLevel,
    /// Set once a source removed the item: the delete propagates to every
    /// source still holding it, and it is never copied to one that lacks it. A
    /// later live upsert clears it (edit- and add-beats-delete across sources).
    pub deleted: bool,
    /// Set when a cross-source content conflict was left unresolved (the
    /// `Manual` policy); the diverging body is in `conflict_object`.
    pub conflicted: bool,
    /// The diverging body a `Manual` conflict recorded, for the consumer to
    /// resolve against; `None` otherwise.
    pub conflict_object: Option<ReplicaHash>,
    /// Per-source bindings, keyed by source id.
    pub sources: BTreeMap<ReplicaSourceId, ReplicaSourceBinding>,
}

/// The multi-source hub: logical items keyed by link id.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ReplicaHub {
    /// The shared items, keyed by their cross-source link id.
    pub items: BTreeMap<ReplicaLinkId, ReplicaHubItem>,
    /// How a cross-source content conflict is resolved.
    pub conflict: ReplicaHubConflict,
}

impl ReplicaHub {
    /// Projects the per-source placements a source's `load` should return.
    ///
    /// Each item bound to `source` yields a placement at the hub's shared
    /// content but the source's own base, so a hub change the source has not
    /// seen reads as dirty and the engine pushes it. Each item the source
    /// lacks, whose body the hub already holds, yields a `Created` append
    /// (membership propagation). Never raises the level, so a two-source sync
    /// of in-agreement items fetches zero bodies.
    pub fn project(
        &self,
        collection: &ReplicaCollectionId,
        source: &ReplicaSourceId,
    ) -> Vec<ReplicaPlacement> {
        let mut out = Vec::new();

        for (link, item) in &self.items {
            match (item.deleted, item.sources.get(source)) {
                // NOTE: deleted upstream, still held here — push the delete
                // to this source. Held nowhere here, or deleted: nothing to
                // project.
                (true, Some(binding)) => {
                    out.push(self.tombstone_placement(collection, link, item, binding));
                }
                (true, None) => {}
                (false, Some(binding)) => {
                    out.push(self.bound_placement(collection, link, item, binding));
                }
                (false, None) => {
                    if let Some(created) = self.created_placement(collection, link, item) {
                        out.push(created);
                    }
                }
            }
        }

        out
    }

    /// The placement for an item this source already holds.
    fn bound_placement(
        &self,
        collection: &ReplicaCollectionId,
        link: &ReplicaLinkId,
        item: &ReplicaHubItem,
        binding: &ReplicaSourceBinding,
    ) -> ReplicaPlacement {
        let in_sync = binding
            .base
            .as_ref()
            .is_some_and(|b| b.flags == item.flags && b.object == item.object);
        let status = if binding.conflicted {
            // NOTE: an unresolved conflict outranks the base comparison. The
            // merge leaves a conflicted placement alone; downgrading it to
            // Dirty here would re-derive the push it already rejected, so the
            // same conflict would be re-marked on every run and never converge.
            ReplicaStatus::Conflict
        } else if in_sync {
            ReplicaStatus::Clean
        } else {
            // NOTE: the hub diverged from this source's base, so push it.
            ReplicaStatus::Dirty
        };

        ReplicaPlacement {
            collection: collection.clone(),
            handle: binding.handle.clone(),
            link_id: Some(link.clone()),
            object: item.object.clone(),
            level: item.level,
            meta: item.meta.clone(),
            flags: item.flags.clone(),
            status,
            conflict_revision: binding.conflict_revision.clone(),
            base: binding.base.clone(),
            origin: None,
        }
    }

    /// A `Tombstone` for an item deleted elsewhere but still held here, so the
    /// source's next sync pushes a `Remove`. The content is kept so the
    /// engine's edit-beats-delete rule can still resurrect it if the source's
    /// server changed it meanwhile.
    fn tombstone_placement(
        &self,
        collection: &ReplicaCollectionId,
        link: &ReplicaLinkId,
        item: &ReplicaHubItem,
        binding: &ReplicaSourceBinding,
    ) -> ReplicaPlacement {
        ReplicaPlacement {
            collection: collection.clone(),
            handle: binding.handle.clone(),
            link_id: Some(link.clone()),
            object: item.object.clone(),
            level: item.level,
            meta: item.meta.clone(),
            flags: item.flags.clone(),
            status: ReplicaStatus::Tombstone,
            conflict_revision: None,
            base: binding.base.clone(),
            origin: None,
        }
    }

    /// A `Created` append for an item missing on this source, staged only when
    /// the body is already hydrated so it never triggers a fetch.
    fn created_placement(
        &self,
        collection: &ReplicaCollectionId,
        link: &ReplicaLinkId,
        item: &ReplicaHubItem,
    ) -> Option<ReplicaPlacement> {
        item.object.as_ref()?;

        let mut handle = link.0.clone();
        handle.push_str("\u{1}hub");

        Some(ReplicaPlacement {
            collection: collection.clone(),
            handle: ReplicaHandle(handle),
            link_id: Some(link.clone()),
            object: item.object.clone(),
            level: ReplicaLevel::Full,
            meta: item.meta.clone(),
            flags: item.flags.clone(),
            status: ReplicaStatus::Created,
            conflict_revision: None,
            base: None,
            origin: None,
        })
    }

    /// Folds a source's sync writes back into the hub: an upsert adopts the
    /// source's reconciled content as the shared content and refreshes that
    /// source's binding; a drop removes the binding. `StoreObject` and
    /// `SetCheckpoint` are the wrapping storage's concern and ignored here.
    pub fn absorb(&mut self, source: &ReplicaSourceId, writes: &[ReplicaWriteOp]) {
        for op in writes {
            match op {
                ReplicaWriteOp::UpsertPlacement(placement) => self.absorb_upsert(source, placement),
                ReplicaWriteOp::DropPlacement { handle, .. } => self.absorb_drop(source, handle),
                ReplicaWriteOp::StoreObject { .. } | ReplicaWriteOp::SetCheckpoint { .. } => {}
            }
        }

        // NOTE: prune items no source holds any more.
        self.items.retain(|_, item| !item.sources.is_empty());
    }

    fn absorb_upsert(&mut self, source: &ReplicaSourceId, placement: &ReplicaPlacement) {
        // NOTE: an unlinked placement cannot be shared across sources yet; it
        // is hubbed once a Meta fetch resolves its link id.
        let Some(link) = placement.link_id.clone() else {
            return;
        };

        let policy = self.conflict;
        let item = self.items.entry(link).or_insert_with(|| ReplicaHubItem {
            flags: placement.flags.clone(),
            object: placement.object.clone(),
            meta: placement.meta.clone(),
            level: placement.level,
            deleted: false,
            conflicted: false,
            conflict_object: None,
            sources: BTreeMap::new(),
        });

        // NOTE: a `Tombstone`-status upsert is a client-staged delete (a
        // `Remove`, or a `Move`'s source side): mark the item deleted and keep
        // this source's binding — its handle and base — so the projection knows
        // the remote handle to push the remove against. Do not adopt the
        // tombstone's content or clear the delete; the kept content lets
        // edit-beats-delete still resurrect it if the source's server changed it.
        if placement.status == ReplicaStatus::Tombstone {
            item.deleted = true;
            item.sources
                .insert(source.clone(), Self::binding_of(placement));
            return;
        }

        // NOTE: a live upsert resurrects a cross-source delete in flight
        // (edit/add beats delete): the item comes back on every source.
        item.deleted = false;

        // NOTE: flags merge element-wise and never conflict; meta and level
        // adopt unconditionally.
        item.flags = placement.flags.clone();
        if placement.meta.is_some() {
            item.meta = placement.meta.clone();
        }
        item.level = item.level.max(placement.level);

        Self::reconcile_content(item, source, placement, policy);

        item.sources
            .insert(source.clone(), Self::binding_of(placement));
    }

    /// The binding an upsert leaves for its source: its handle and base, plus
    /// whether this source's own sync is stuck on a conflict with its remote.
    ///
    /// Recording the conflict is what makes the round trip faithful: the merge
    /// leaves an unresolved conflict alone, so a projection that reported it as
    /// `Dirty` would re-derive the rejected push every run. Any status other
    /// than `Conflict` clears it, which is how a consumer's resolving edit
    /// (absorbed as `Dirty`) ends the conflict without a dedicated call.
    fn binding_of(placement: &ReplicaPlacement) -> ReplicaSourceBinding {
        let conflicted = placement.status == ReplicaStatus::Conflict;
        ReplicaSourceBinding {
            handle: placement.handle.clone(),
            base: placement.base.clone(),
            conflicted,
            // Only meaningful while conflicted; dropping it otherwise keeps a
            // resolved binding from carrying a stale revision forward.
            conflict_revision: conflicted
                .then(|| placement.conflict_revision.clone())
                .flatten(),
        }
    }

    /// Reconciles the shared body against an incoming upsert. A clean
    /// fast-forward (only the source changed since it last agreed) adopts the
    /// body; a divergence (both moved to different bodies) resolves by policy.
    fn reconcile_content(
        item: &mut ReplicaHubItem,
        source: &ReplicaSourceId,
        placement: &ReplicaPlacement,
        policy: ReplicaHubConflict,
    ) {
        let prev = item
            .sources
            .get(source)
            .and_then(|b| b.base.as_ref())
            .and_then(|b| b.object.clone());
        let shared = item.object.clone();
        let incoming = placement.object.clone();

        let source_edited = incoming != prev;
        let hub_moved = shared != prev;
        let diverged = source_edited
            && hub_moved
            && incoming != shared
            && incoming.is_some()
            && shared.is_some();

        if diverged {
            match policy {
                ReplicaHubConflict::Manual => {
                    item.conflicted = true;
                    item.conflict_object = incoming;
                }
                ReplicaHubConflict::PreferIncoming => {
                    item.object = incoming;
                    item.conflicted = false;
                    item.conflict_object = None;
                }
                ReplicaHubConflict::PreferExisting => {
                    item.conflicted = false;
                    item.conflict_object = None;
                }
            }
        } else if source_edited && !hub_moved {
            item.object = incoming;
            item.conflicted = false;
            item.conflict_object = None;
        }
        // NOTE: else the source is unchanged or behind the hub: keep the shared
        // body, which the next projection pushes to the lagging source.
    }

    fn absorb_drop(&mut self, source: &ReplicaSourceId, handle: &ReplicaHandle) {
        for item in self.items.values_mut() {
            let bound_here = item
                .sources
                .get(source)
                .is_some_and(|binding| &binding.handle == handle);
            if bound_here {
                // NOTE: a bound member removed is a genuine delete: mark the
                // item so it propagates to the sources that still hold it.
                item.deleted = true;
                item.sources.remove(source);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use alloc::vec::Vec;

    use crate::{
        change::ReplicaWriteOp,
        hub::*,
        object::ReplicaHash,
        placement::{
            ReplicaBase, ReplicaFlags, ReplicaHandle, ReplicaLevel, ReplicaLinkId,
            ReplicaPlacement, ReplicaStatus,
        },
    };

    fn base(flags: &[&str]) -> ReplicaBase {
        ReplicaBase {
            flags: ReplicaFlags::from_iter(flags.iter().copied()),
            revision: None,
            object: None,
        }
    }

    /// A hub with one item on `left`, in sync at `Meta` with no body.
    fn hub_with_left(flags: &[&str]) -> ReplicaHub {
        let mut sources = BTreeMap::new();
        sources.insert(
            ReplicaSourceId::from("left"),
            ReplicaSourceBinding {
                handle: ReplicaHandle::from("l1"),
                base: Some(base(flags)),
                conflicted: false,
                conflict_revision: None,
            },
        );
        let item = ReplicaHubItem {
            flags: ReplicaFlags::from_iter(flags.iter().copied()),
            object: None,
            meta: None,
            level: ReplicaLevel::Meta,
            deleted: false,
            conflicted: false,
            conflict_object: None,
            sources,
        };
        ReplicaHub {
            items: [(ReplicaLinkId::from("m1"), item)].into_iter().collect(),
            ..Default::default()
        }
    }

    fn placements(hub: &ReplicaHub, source: &str) -> Vec<ReplicaPlacement> {
        hub.project(&"inbox".into(), &ReplicaSourceId::from(source))
    }

    /// Binds `right` to the single item at the given base flags.
    fn bind_right(hub: &mut ReplicaHub, flags: &[&str]) {
        hub.items
            .get_mut(&ReplicaLinkId::from("m1"))
            .unwrap()
            .sources
            .insert(
                ReplicaSourceId::from("right"),
                ReplicaSourceBinding {
                    handle: ReplicaHandle::from("r1"),
                    base: Some(base(flags)),
                    conflicted: false,
                    conflict_revision: None,
                },
            );
    }

    #[test]
    fn in_agreement_items_project_clean_without_a_body() {
        // NOTE: the zero-bodies guardrail — an item both sides agree on
        // projects Clean, at its current level, with no object, so the engine
        // fetches nothing.
        let mut hub = hub_with_left(&["seen"]);
        hub.items
            .get_mut(&ReplicaLinkId::from("m1"))
            .unwrap()
            .sources
            .insert(
                ReplicaSourceId::from("right"),
                ReplicaSourceBinding {
                    handle: ReplicaHandle::from("r1"),
                    base: Some(base(&["seen"])),
                    conflicted: false,
                    conflict_revision: None,
                },
            );

        for source in ["left", "right"] {
            let projected = placements(&hub, source);
            assert_eq!(projected.len(), 1);
            assert_eq!(projected[0].status, ReplicaStatus::Clean);
            assert_ne!(projected[0].level, ReplicaLevel::Full);
            assert_eq!(projected[0].object, None, "no body demanded");
        }
    }

    #[test]
    fn a_flag_change_absorbed_from_one_source_projects_dirty_on_the_other() {
        // NOTE: left and right both hold the item, agreeing on []. left's
        // server adds "seen": absorbing left's write makes the hub dirty
        // against right's base, so right's projection pushes it.
        let mut hub = hub_with_left(&[]);
        hub.items
            .get_mut(&ReplicaLinkId::from("m1"))
            .unwrap()
            .sources
            .insert(
                ReplicaSourceId::from("right"),
                ReplicaSourceBinding {
                    handle: ReplicaHandle::from("r1"),
                    base: Some(base(&[])),
                    conflicted: false,
                    conflict_revision: None,
                },
            );

        // NOTE: left pulled "seen" from its server, a clean placement
        // carrying it.
        let mut pulled = placements(&hub, "left").pop().unwrap();
        pulled.flags = ReplicaFlags::from_iter(["seen"]);
        pulled.status = ReplicaStatus::Clean;
        pulled.base = Some(base(&["seen"]));
        hub.absorb(
            &ReplicaSourceId::from("left"),
            &[ReplicaWriteOp::UpsertPlacement(pulled)],
        );

        let right = placements(&hub, "right").pop().unwrap();
        assert!(right.flags.contains("seen"), "hub adopted left's change");
        assert_eq!(right.status, ReplicaStatus::Dirty, "right must push it");
        assert_eq!(
            right.base.unwrap().flags,
            ReplicaFlags::from_iter([] as [&str; 0]),
            "right's base is untouched, so the merge pushes to right",
        );
    }

    #[test]
    fn an_item_missing_on_a_source_projects_a_created_append_once_hydrated() {
        let mut hub = hub_with_left(&["seen"]);
        // NOTE: give the item a body (hydrated), still only on left.
        hub.items
            .get_mut(&ReplicaLinkId::from("m1"))
            .unwrap()
            .object = Some(ReplicaHash::from("h1"));
        hub.items.get_mut(&ReplicaLinkId::from("m1")).unwrap().level = ReplicaLevel::Full;

        let right = placements(&hub, "right");
        assert_eq!(right.len(), 1);
        assert_eq!(right[0].status, ReplicaStatus::Created);
        assert_eq!(right[0].object, Some(ReplicaHash::from("h1")));
        assert!(right[0].base.is_none());
    }

    #[test]
    fn a_missing_item_without_a_body_is_not_projected_no_fetch() {
        // NOTE: no body yet, so the append is not staged and nothing forces
        // a fetch.
        let hub = hub_with_left(&["seen"]);
        assert!(placements(&hub, "right").is_empty());
    }

    #[test]
    fn absorbing_a_drop_removes_only_that_sources_binding() {
        let mut hub = hub_with_left(&["seen"]);
        hub.items
            .get_mut(&ReplicaLinkId::from("m1"))
            .unwrap()
            .sources
            .insert(
                ReplicaSourceId::from("right"),
                ReplicaSourceBinding {
                    handle: ReplicaHandle::from("r1"),
                    base: Some(base(&["seen"])),
                    conflicted: false,
                    conflict_revision: None,
                },
            );

        hub.absorb(
            &ReplicaSourceId::from("left"),
            &[ReplicaWriteOp::DropPlacement {
                collection: "inbox".into(),
                handle: ReplicaHandle::from("l1"),
            }],
        );

        let item = hub.items.get(&ReplicaLinkId::from("m1")).expect("kept");
        assert!(!item.sources.contains_key(&ReplicaSourceId::from("left")));
        assert!(item.sources.contains_key(&ReplicaSourceId::from("right")));
    }

    #[test]
    fn a_delete_on_one_source_projects_a_tombstone_on_the_other() {
        let mut hub = hub_with_left(&["seen"]);
        bind_right(&mut hub, &["seen"]);

        // NOTE: left's server expunged the item: the engine drops it on left.
        hub.absorb(
            &ReplicaSourceId::from("left"),
            &[ReplicaWriteOp::DropPlacement {
                collection: "inbox".into(),
                handle: ReplicaHandle::from("l1"),
            }],
        );

        let item = hub.items.get(&ReplicaLinkId::from("m1")).expect("kept");
        assert!(item.deleted, "the delete is recorded");
        assert!(!item.sources.contains_key(&ReplicaSourceId::from("left")));

        // NOTE: right still holds it, so it projects a Tombstone to push a
        // Remove.
        let right = placements(&hub, "right");
        assert_eq!(right.len(), 1);
        assert_eq!(right[0].status, ReplicaStatus::Tombstone);
        assert_eq!(right[0].handle.as_str(), "r1");
        assert!(
            right[0].base.is_some(),
            "based, so the engine pushes a remove"
        );
        // NOTE: left no longer holds it, so it projects nothing (not a
        // re-copy).
        assert!(placements(&hub, "left").is_empty());
    }

    #[test]
    fn a_client_staged_tombstone_upsert_marks_deleted_and_keeps_the_binding() {
        // NOTE: a Remove (or a Move's source side) stages a Tombstone-status
        // upsert, not a drop. Absorbing it must mark the item deleted while
        // keeping the binding, so the projection pushes the remove — unlike a
        // live upsert, which would resurrect it.
        let mut hub = hub_with_left(&["seen"]);

        let mut tombstone = placements(&hub, "left").pop().unwrap();
        tombstone.status = ReplicaStatus::Tombstone;
        hub.absorb(
            &ReplicaSourceId::from("left"),
            &[ReplicaWriteOp::UpsertPlacement(tombstone)],
        );

        let item = hub.items.get(&ReplicaLinkId::from("m1")).expect("kept");
        assert!(item.deleted, "the staged delete is recorded");
        assert!(
            item.sources.contains_key(&ReplicaSourceId::from("left")),
            "the binding is kept so the projection knows the remote handle",
        );

        let left = placements(&hub, "left");
        assert_eq!(left.len(), 1);
        assert_eq!(left[0].status, ReplicaStatus::Tombstone);
        assert_eq!(left[0].handle.as_str(), "l1");
        assert!(
            left[0].base.is_some(),
            "based, so the engine pushes a remove"
        );
    }

    #[test]
    fn a_deleted_item_is_pruned_once_every_source_propagates() {
        let mut hub = hub_with_left(&["seen"]);
        bind_right(&mut hub, &["seen"]);

        for (source, handle) in [("left", "l1"), ("right", "r1")] {
            hub.absorb(
                &ReplicaSourceId::from(source),
                &[ReplicaWriteOp::DropPlacement {
                    collection: "inbox".into(),
                    handle: ReplicaHandle::from(handle),
                }],
            );
        }

        assert!(hub.items.is_empty(), "no source holds it, so it is pruned");
    }

    #[test]
    fn a_live_upsert_resurrects_a_delete_in_flight() {
        let mut hub = hub_with_left(&["seen"]);
        bind_right(&mut hub, &["seen"]);

        // NOTE: right deletes it.
        hub.absorb(
            &ReplicaSourceId::from("right"),
            &[ReplicaWriteOp::DropPlacement {
                collection: "inbox".into(),
                handle: ReplicaHandle::from("r1"),
            }],
        );
        assert!(hub.items.get(&ReplicaLinkId::from("m1")).unwrap().deleted);

        // NOTE: left's server had edited it (edit-beats-delete), so left's sync
        // resurrects it as a live upsert rather than pushing the delete.
        let mut pulled = ReplicaPlacement {
            collection: "inbox".into(),
            handle: ReplicaHandle::from("l1"),
            link_id: Some(ReplicaLinkId::from("m1")),
            object: None,
            level: ReplicaLevel::Meta,
            meta: None,
            flags: ReplicaFlags::from_iter(["seen", "flagged"]),
            status: ReplicaStatus::Clean,
            conflict_revision: None,
            base: Some(base(&["seen", "flagged"])),
            origin: None,
        };
        hub.absorb(
            &ReplicaSourceId::from("left"),
            &[ReplicaWriteOp::UpsertPlacement(pulled.clone())],
        );

        let item = hub
            .items
            .get(&ReplicaLinkId::from("m1"))
            .expect("resurrected");
        assert!(!item.deleted, "a live upsert clears the delete");
        assert!(item.flags.contains("flagged"));
        // NOTE: right lacks it now, so it projects a Created copy (resurrected
        // there).
        pulled.object = Some(ReplicaHash::from("h1"));
        hub.items
            .get_mut(&ReplicaLinkId::from("m1"))
            .unwrap()
            .object = Some(ReplicaHash::from("h1"));
        assert_eq!(
            placements(&hub, "right")[0].status,
            ReplicaStatus::Created,
            "the resurrected item copies back to right",
        );
    }

    /// A hub with one mutable item (body `o0`) held by left and right, both
    /// last-synced against `o0`, with the given cross-source conflict policy.
    fn content_hub(policy: ReplicaHubConflict) -> ReplicaHub {
        let based = |handle: &str| ReplicaSourceBinding {
            handle: ReplicaHandle::from(handle),
            base: Some(ReplicaBase {
                flags: ReplicaFlags::default(),
                revision: Some("r0".into()),
                object: Some(ReplicaHash::from("o0")),
            }),
            conflicted: false,
            conflict_revision: None,
        };
        let mut sources = BTreeMap::new();
        sources.insert(ReplicaSourceId::from("left"), based("l1"));
        sources.insert(ReplicaSourceId::from("right"), based("r1"));
        let item = ReplicaHubItem {
            flags: ReplicaFlags::default(),
            object: Some(ReplicaHash::from("o0")),
            meta: None,
            level: ReplicaLevel::Full,
            deleted: false,
            conflicted: false,
            conflict_object: None,
            sources,
        };
        ReplicaHub {
            items: [(ReplicaLinkId::from("m1"), item)].into_iter().collect(),
            conflict: policy,
        }
    }

    /// An upsert write for the shared item from `handle`, carrying `object`.
    fn content_upsert(handle: &str, object: &str) -> ReplicaWriteOp {
        ReplicaWriteOp::UpsertPlacement(ReplicaPlacement {
            collection: "inbox".into(),
            handle: ReplicaHandle::from(handle),
            link_id: Some(ReplicaLinkId::from("m1")),
            object: Some(ReplicaHash::from(object)),
            level: ReplicaLevel::Full,
            meta: None,
            flags: ReplicaFlags::default(),
            status: ReplicaStatus::Clean,
            conflict_revision: None,
            base: Some(ReplicaBase {
                flags: ReplicaFlags::default(),
                revision: Some("r1".into()),
                object: Some(ReplicaHash::from(object)),
            }),
            origin: None,
        })
    }

    fn shared_object(hub: &ReplicaHub) -> Option<ReplicaHash> {
        hub.items
            .get(&ReplicaLinkId::from("m1"))
            .unwrap()
            .object
            .clone()
    }

    #[test]
    fn a_clean_fast_forward_adopts_the_new_body() {
        // NOTE: only left edited since both agreed on o0, so no conflict —
        // adopt it.
        let mut hub = content_hub(ReplicaHubConflict::Manual);
        hub.absorb(
            &ReplicaSourceId::from("left"),
            &[content_upsert("l1", "oa")],
        );
        assert_eq!(shared_object(&hub), Some(ReplicaHash::from("oa")));
        assert!(
            !hub.items
                .get(&ReplicaLinkId::from("m1"))
                .unwrap()
                .conflicted
        );
    }

    #[test]
    fn divergent_content_conflicts_and_preserves_both_manual() {
        // NOTE: left edited to oa (adopted), then right edited to ob against
        // the old o0 base: both moved, so Manual flags it and records ob,
        // keeping oa.
        let mut hub = content_hub(ReplicaHubConflict::Manual);
        hub.absorb(
            &ReplicaSourceId::from("left"),
            &[content_upsert("l1", "oa")],
        );
        hub.absorb(
            &ReplicaSourceId::from("right"),
            &[content_upsert("r1", "ob")],
        );

        let item = hub.items.get(&ReplicaLinkId::from("m1")).unwrap();
        assert!(item.conflicted, "the divergence is detected");
        assert_eq!(
            item.object,
            Some(ReplicaHash::from("oa")),
            "the shared body is kept"
        );
        assert_eq!(
            item.conflict_object,
            Some(ReplicaHash::from("ob")),
            "the diverging body is preserved for resolution",
        );
    }

    #[test]
    fn prefer_incoming_takes_the_last_writer() {
        let mut hub = content_hub(ReplicaHubConflict::PreferIncoming);
        hub.absorb(
            &ReplicaSourceId::from("left"),
            &[content_upsert("l1", "oa")],
        );
        hub.absorb(
            &ReplicaSourceId::from("right"),
            &[content_upsert("r1", "ob")],
        );

        let item = hub.items.get(&ReplicaLinkId::from("m1")).unwrap();
        assert!(!item.conflicted);
        assert_eq!(
            item.object,
            Some(ReplicaHash::from("ob")),
            "last writer wins"
        );
    }

    #[test]
    fn prefer_existing_keeps_the_shared_body() {
        let mut hub = content_hub(ReplicaHubConflict::PreferExisting);
        hub.absorb(
            &ReplicaSourceId::from("left"),
            &[content_upsert("l1", "oa")],
        );
        hub.absorb(
            &ReplicaSourceId::from("right"),
            &[content_upsert("r1", "ob")],
        );

        let item = hub.items.get(&ReplicaLinkId::from("m1")).unwrap();
        assert!(!item.conflicted);
        assert_eq!(
            item.object,
            Some(ReplicaHash::from("oa")),
            "the shared body is kept"
        );
    }

    /// A conflicted upsert for the shared item from `handle`: the local body is
    /// `object`, and `revision` is the remote revision the merge observed.
    fn conflicted_upsert(handle: &str, object: &str, revision: &str) -> ReplicaWriteOp {
        ReplicaWriteOp::UpsertPlacement(ReplicaPlacement {
            collection: "inbox".into(),
            handle: ReplicaHandle::from(handle),
            link_id: Some(ReplicaLinkId::from("m1")),
            object: Some(ReplicaHash::from(object)),
            level: ReplicaLevel::Full,
            meta: None,
            flags: ReplicaFlags::default(),
            status: ReplicaStatus::Conflict,
            conflict_revision: Some(revision.into()),
            base: Some(ReplicaBase {
                flags: ReplicaFlags::default(),
                revision: Some("r0".into()),
                object: Some(ReplicaHash::from("o0")),
            }),
            origin: None,
        })
    }

    #[test]
    fn a_conflicted_placement_round_trips_with_its_revision() {
        // The bug this change fixes: the merge marks a placement Conflict and
        // records the remote revision it saw, and both must come back out. Read
        // back as Dirty, the engine re-derives the push it already had rejected
        // and re-conflicts on every run, never converging.
        let mut hub = content_hub(ReplicaHubConflict::Manual);
        hub.absorb(
            &ReplicaSourceId::from("left"),
            &[conflicted_upsert("l1", "o-local", "r-remote")],
        );

        let projected = placements(&hub, "left");
        assert_eq!(projected.len(), 1);
        assert_eq!(projected[0].status, ReplicaStatus::Conflict);
        assert_eq!(
            projected[0].conflict_revision.as_deref(),
            Some("r-remote"),
            "the observed remote revision is what a resolver merges against"
        );
    }

    #[test]
    fn a_conflict_outranks_the_base_comparison() {
        // A conflicted binding whose base still matches the shared content would
        // otherwise project Clean, silently losing the conflict; and one that
        // diverges would project Dirty, re-deriving the rejected push. Neither
        // may happen.
        let mut hub = content_hub(ReplicaHubConflict::Manual);
        hub.absorb(
            &ReplicaSourceId::from("left"),
            &[conflicted_upsert("l1", "o0", "r-remote")],
        );

        let binding =
            hub.items[&ReplicaLinkId::from("m1")].sources[&ReplicaSourceId::from("left")].clone();
        assert!(binding.conflicted);
        // The base equals the shared content, so the Clean branch would win
        // without the conflict check.
        assert_eq!(
            binding.base.as_ref().and_then(|b| b.object.clone()),
            hub.items[&ReplicaLinkId::from("m1")].object
        );
        assert_eq!(placements(&hub, "left")[0].status, ReplicaStatus::Conflict);
    }

    #[test]
    fn resolving_the_conflict_with_an_edit_clears_it() {
        // Resolution needs no dedicated call: the consumer's edit arrives as an
        // ordinary upsert, and any status but Conflict clears the binding.
        let mut hub = content_hub(ReplicaHubConflict::Manual);
        let left = ReplicaSourceId::from("left");
        hub.absorb(&left, &[conflicted_upsert("l1", "o-local", "r-remote")]);
        assert_eq!(placements(&hub, "left")[0].status, ReplicaStatus::Conflict);

        hub.absorb(&left, &[content_upsert("l1", "o-merged")]);

        let binding = hub.items[&ReplicaLinkId::from("m1")].sources[&left].clone();
        assert!(!binding.conflicted);
        assert_eq!(
            binding.conflict_revision, None,
            "a resolved binding must not carry a stale revision forward"
        );
        assert_ne!(placements(&hub, "left")[0].status, ReplicaStatus::Conflict);
    }

    #[test]
    fn the_two_conflict_axes_stay_independent() {
        // A source-vs-its-own-remote conflict and a cross-source conflict are
        // different facts; conflating them would make a two-source store report
        // one as the other. Neither may leak into the other's flag.
        let mut hub = content_hub(ReplicaHubConflict::Manual);
        let link = ReplicaLinkId::from("m1");

        // Left conflicts with its own server: the binding is marked, the shared
        // item's cross-source flag is not.
        hub.absorb(
            &ReplicaSourceId::from("left"),
            &[conflicted_upsert("l1", "o-local", "r-remote")],
        );
        assert!(hub.items[&link].sources[&ReplicaSourceId::from("left")].conflicted);
        assert!(
            !hub.items[&link].conflicted,
            "a per-source conflict is not a cross-source one"
        );

        // Right's binding is untouched by left's conflict.
        assert!(!hub.items[&link].sources[&ReplicaSourceId::from("right")].conflicted);
        assert_eq!(placements(&hub, "right")[0].status, ReplicaStatus::Dirty);

        // Now a genuine cross-source divergence (both moved to different
        // bodies): the item is flagged, and right's own binding is not.
        let mut hub = content_hub(ReplicaHubConflict::Manual);
        hub.absorb(
            &ReplicaSourceId::from("left"),
            &[content_upsert("l1", "o-l")],
        );
        hub.absorb(
            &ReplicaSourceId::from("right"),
            &[content_upsert("r1", "o-r")],
        );
        assert!(hub.items[&link].conflicted);
        assert_eq!(
            hub.items[&link].conflict_object,
            Some(ReplicaHash::from("o-r"))
        );
        assert!(
            !hub.items[&link].sources[&ReplicaSourceId::from("right")].conflicted,
            "a cross-source conflict is not a per-source one"
        );
    }

    #[test]
    fn a_tombstone_is_never_conflicted() {
        // The tombstone path takes the same binding constructor; a staged
        // delete carries no conflict, and must not inherit a stale one.
        let mut hub = content_hub(ReplicaHubConflict::Manual);
        let left = ReplicaSourceId::from("left");
        hub.absorb(&left, &[conflicted_upsert("l1", "o-local", "r-remote")]);

        let mut tombstone = match content_upsert("l1", "o0") {
            ReplicaWriteOp::UpsertPlacement(p) => p,
            _ => unreachable!(),
        };
        tombstone.status = ReplicaStatus::Tombstone;
        hub.absorb(&left, &[ReplicaWriteOp::UpsertPlacement(tombstone)]);

        let binding = hub.items[&ReplicaLinkId::from("m1")].sources[&left].clone();
        assert!(!binding.conflicted);
        assert_eq!(binding.conflict_revision, None);
    }
}
