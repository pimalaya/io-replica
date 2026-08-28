//! Multi-source hub: shared content with a base per source.
//!
//! One logical item can live on several sources (a `left` and a `right`
//! server, a server and a phone). The hub holds that item's current
//! content once, plus a last-synced base per source, so a storage
//! wrapping it can [`project`] a per-source placement and [`absorb`] the
//! engine's writes back. A projected placement carries the shared
//! content against the source's own base, so a change another source
//! folded in reads as locally dirty here and the ordinary reconcile
//! pushes it: propagation falls out of the per-source merge, with no
//! cross-merge. Adds, flags and deletes propagate the same way;
//! cross-source content conflicts resolve by policy.
//!
//! [`project`]: ReplicaHub::project
//! [`absorb`]: ReplicaHub::absorb

use alloc::{collections::BTreeMap, string::String, vec::Vec};

use crate::{
    change::{ReplicaDropReason, ReplicaWriteOp},
    collection::ReplicaCollectionId,
    object::ReplicaHash,
    placement::{
        ReplicaBase, ReplicaFlags, ReplicaHandle, ReplicaLevel, ReplicaLinkId, ReplicaMeta,
        ReplicaPlacement, ReplicaSortKey, ReplicaStatus,
    },
};

crate::replica_id! {
    /// A source of a shared item: one authoritative replica (`left`, `right`,
    /// `phone`).
    ReplicaSourceId, Ord, PartialOrd,
}

/// One source's binding of a shared item: its handle there, the base last
/// synced with it, and whether that source's own sync is stuck on a conflict.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReplicaSourceBinding {
    /// The item's handle on this source.
    pub handle: ReplicaHandle,
    /// The last state synced with this source; `None` until first reconciled.
    pub base: Option<ReplicaBase>,
    /// Set when this source and its own remote diverged and the merge
    /// left the placement [`ReplicaStatus::Conflict`].
    ///
    /// Distinct from [`ReplicaHubItem::conflicted`], the cross-source
    /// conflict: one says left and its server disagree, the other left
    /// and right disagree, and a two-source store needs both. Cleared by
    /// an upsert of any other status, so a consumer resolving with an
    /// edit needs no explicit resolution call.
    pub conflicted: bool,
    /// The remote revision observed when this binding was marked
    /// conflicted, what a resolver merges against. `None` when not
    /// conflicted, or when the remote reports no revision.
    pub conflict_revision: Option<String>,
    /// The remote body at that revision, so a resolver reads the
    /// divergence from the store rather than from the source. `None`
    /// until the upgrade pass supplies it, and dropped whenever the
    /// revision beside it moves.
    ///
    /// Persisted with the binding because that is what a storage keeps
    /// per source: the projection hands it back to the placement, and
    /// an absorb records whatever the placement holds.
    pub conflict_object: Option<ReplicaHash>,
    /// The shared body this source last reconciled against, which is
    /// the base of the cross-source merge.
    ///
    /// The second axis needs a base of its own. [`base`](Self::base) is
    /// what this source last agreed with its own remote, and only a
    /// sync moves it, so a body this source folded into the hub and has
    /// not pushed yet leaves it behind the shared one. Read as the
    /// cross-source base it would make the source disagree with itself,
    /// and its next edit would be dropped as a conflict.
    ///
    /// `None` until this source has folded once, where the sync base
    /// stands in for it.
    pub shared_object: Option<ReplicaHash>,
}

/// How the hub resolves a cross-source content conflict, both sources
/// having edited the same body since they last agreed.
///
/// Flags never reach this: they merge element-wise. Only mutable-content
/// backends conflict; immutable ones mint a new link id per body.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum ReplicaHubConflict {
    /// Flag the item conflicted and record the diverging body for the
    /// consumer to resolve, keeping the shared body so nothing is lost.
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
    /// The current sort key, shared by every source; empty until derived.
    pub sort_key: ReplicaSortKey,
    /// The highest detail level any source has reached, which is the
    /// item's own only while it holds a body
    /// ([`stored_level`](Self::stored_level)).
    pub level: ReplicaLevel,
    /// Set once a source removed the item: the delete propagates to
    /// every source still holding it, and it is never copied to one that
    /// lacks it. A later live upsert clears it, edit and add beating
    /// delete across sources.
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

impl ReplicaHubItem {
    /// The detail level the item can honestly claim: [`Full`] means a
    /// stored body, so an item holding none reads one rung down however
    /// far a source got.
    ///
    /// [`level`](Self::level) is the high-water mark across sources, and
    /// only [`object`](Self::object) says whether the body is there.
    /// Reading the mark as the fact strands an item a content change
    /// refreshed, an upgrade skipping whatever reads as [`Full`].
    ///
    /// [`Full`]: ReplicaLevel::Full
    pub fn stored_level(&self) -> ReplicaLevel {
        match self.object {
            Some(_) => self.level,
            None => self.level.min(ReplicaLevel::Meta),
        }
    }

    /// The placement this item projects into `collection` under `handle`.
    ///
    /// The shared half of a projection, the same for every source: the
    /// content the hub holds, at the level it can honestly claim. What a
    /// binding decides (status, base, conflict revision) is settled by
    /// the caller.
    fn project(
        &self,
        collection: &ReplicaCollectionId,
        link: &ReplicaLinkId,
        handle: ReplicaHandle,
    ) -> ReplicaPlacement {
        ReplicaPlacement {
            collection: collection.clone(),
            handle,
            link_id: Some(link.clone()),
            object: self.object.clone(),
            level: self.stored_level(),
            meta: self.meta.clone(),
            sort_key: self.sort_key.clone(),
            flags: self.flags.clone(),
            status: ReplicaStatus::Clean,
            conflict_revision: None,
            conflict_object: None,
            base: None,
            origin: None,
        }
    }
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
    /// Each item bound to `source` yields a placement at the hub's
    /// shared content but the source's own base, so a hub change the
    /// source has not seen reads as dirty and the engine pushes it. Each
    /// item the source lacks, whose body the hub already holds, yields a
    /// `Created` append. The level is never raised, so a two-source sync
    /// of in-agreement items fetches zero bodies, and the projected
    /// [`stored_level`](ReplicaHubItem::stored_level) makes an item whose
    /// body a content change dropped fetch again.
    pub fn project(
        &self,
        collection: &ReplicaCollectionId,
        source: &ReplicaSourceId,
    ) -> Vec<ReplicaPlacement> {
        let mut out = Vec::new();

        for (link, item) in &self.items {
            match (item.deleted, item.sources.get(source)) {
                // NOTE: deleted elsewhere but still held here, so this
                // source gets the delete pushed to it.
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
            // NOTE: an unresolved conflict outranks the base comparison.
            // Downgrading it to Dirty would re-derive the push the merge
            // already rejected, re-marking the same conflict every run
            // without ever converging.
            ReplicaStatus::Conflict
        } else if in_sync {
            ReplicaStatus::Clean
        } else {
            ReplicaStatus::Dirty
        };

        let mut placement = item.project(collection, link, binding.handle.clone());
        placement.status = status;
        placement.conflict_revision = binding.conflict_revision.clone();
        placement.conflict_object = binding.conflict_object.clone();
        placement.base = binding.base.clone();
        placement
    }

    /// A `Tombstone` for an item deleted elsewhere but still held here,
    /// so the source's next sync pushes a `Remove`. The content is kept
    /// so edit-beats-delete can still resurrect it if the source's
    /// server changed it meanwhile.
    fn tombstone_placement(
        &self,
        collection: &ReplicaCollectionId,
        link: &ReplicaLinkId,
        item: &ReplicaHubItem,
        binding: &ReplicaSourceBinding,
    ) -> ReplicaPlacement {
        let mut placement = item.project(collection, link, binding.handle.clone());
        placement.status = ReplicaStatus::Tombstone;
        placement.base = binding.base.clone();
        placement
    }

    /// A `Created` append for an item missing on this source, staged
    /// only when the body is already hydrated so it never triggers a
    /// fetch.
    fn created_placement(
        &self,
        collection: &ReplicaCollectionId,
        link: &ReplicaLinkId,
        item: &ReplicaHubItem,
    ) -> Option<ReplicaPlacement> {
        item.object.as_ref()?;

        let mut handle = link.0.clone();
        handle.push_str("\u{1}hub");

        let mut placement = item.project(collection, link, ReplicaHandle(handle));
        placement.status = ReplicaStatus::Created;
        // NOTE: the body is there, checked above, so the staged copy
        // claims it whatever high-water mark the item carries.
        placement.level = ReplicaLevel::Full;
        Some(placement)
    }

    /// Folds a source's sync writes back into the hub: an upsert adopts
    /// the reconciled content as the shared content and refreshes that
    /// source's binding, a drop removes the binding. `StoreObject` and
    /// `SetCheckpoint` are the wrapping storage's concern.
    pub fn absorb(&mut self, source: &ReplicaSourceId, writes: &[ReplicaWriteOp]) {
        for op in writes {
            match op {
                ReplicaWriteOp::UpsertPlacement(placement) => self.absorb_upsert(source, placement),
                ReplicaWriteOp::DropPlacement { handle, reason, .. } => {
                    self.absorb_drop(source, handle, *reason)
                }
                ReplicaWriteOp::StoreObject { .. } | ReplicaWriteOp::SetCheckpoint { .. } => {}
            }
        }

        self.items.retain(|_, item| !item.sources.is_empty());
    }

    fn absorb_upsert(&mut self, source: &ReplicaSourceId, placement: &ReplicaPlacement) {
        // NOTE: an unlinked placement cannot be shared across sources
        // yet; it is hubbed once a Meta fetch resolves its link id.
        let Some(link) = placement.link_id.clone() else {
            return;
        };

        let policy = self.conflict;
        let item = self.items.entry(link).or_insert_with(|| ReplicaHubItem {
            flags: placement.flags.clone(),
            object: placement.object.clone(),
            meta: placement.meta.clone(),
            sort_key: placement.sort_key.clone(),
            level: placement.level,
            deleted: false,
            conflicted: false,
            conflict_object: None,
            sources: BTreeMap::new(),
        });

        // NOTE: a `Tombstone` upsert is a client-staged delete, so the
        // binding is kept for the handle the projection pushes the
        // remove against. Its content is not adopted and the delete not
        // cleared: the kept content lets edit-beats-delete resurrect it
        // if the source's server changed it.
        if placement.status == ReplicaStatus::Tombstone {
            let agreed = item
                .sources
                .get(source)
                .and_then(|binding| binding.shared_object.clone());

            item.deleted = true;
            item.sources
                .insert(source.clone(), Self::binding_of(placement, agreed));
            return;
        }

        // NOTE: a live upsert resurrects a cross-source delete in
        // flight: the item comes back on every source.
        item.deleted = false;

        // NOTE: an unknown flag set does not erase a known one: a source
        // that has only probed an item holds no opinion about its
        // markers, and reading that as "no markers" would clear what
        // another source read.
        if !placement.flags.is_unknown() {
            item.flags = placement.flags.clone();
        }
        if placement.meta.is_some() {
            item.meta = placement.meta.clone();
        }
        // NOTE: same for the sort key: a source that has not derived one
        // must not un-sort an item another source already placed.
        if !placement.sort_key.is_unknown() {
            item.sort_key = placement.sort_key.clone();
        }
        item.level = item.level.max(placement.level);

        Self::reconcile_content(item, source, placement, policy);

        item.level = item.stored_level();

        // NOTE: the reconcile has settled the shared body, so this
        // source has now agreed with whatever it left, adopted or not.
        let agreed = item.object.clone();

        item.sources
            .insert(source.clone(), Self::binding_of(placement, agreed));
    }

    /// The binding an upsert leaves for its source: its handle, its two
    /// bases (the one it last synced with its remote and the shared body
    /// it last reconciled against), plus whether this source's own sync
    /// is stuck on a conflict.
    ///
    /// Recording the conflict is what makes the round trip faithful: the
    /// merge leaves an unresolved conflict alone, so a projection
    /// reporting it as `Dirty` would re-derive the rejected push every
    /// run. Any other status clears it, which is how a resolving edit
    /// ends the conflict without a dedicated call.
    fn binding_of(
        placement: &ReplicaPlacement,
        shared_object: Option<ReplicaHash>,
    ) -> ReplicaSourceBinding {
        let conflicted = placement.status == ReplicaStatus::Conflict;
        ReplicaSourceBinding {
            handle: placement.handle.clone(),
            base: placement.base.clone(),
            conflicted,
            conflict_revision: conflicted
                .then(|| placement.conflict_revision.clone())
                .flatten(),
            conflict_object: conflicted
                .then(|| placement.conflict_object.clone())
                .flatten(),
            shared_object,
        }
    }

    /// Reconciles the shared body against an incoming upsert. A clean
    /// fast-forward adopts the body; a divergence resolves by policy.
    ///
    /// Each axis is measured against its own base. The source changed
    /// its body when the upsert differs from what it last synced with
    /// its own remote, and another source moved the shared body when the
    /// item differs from what this source last reconciled against. A
    /// source is therefore never in conflict with a body it folded in
    /// itself, however far behind its remote it is.
    fn reconcile_content(
        item: &mut ReplicaHubItem,
        source: &ReplicaSourceId,
        placement: &ReplicaPlacement,
        policy: ReplicaHubConflict,
    ) {
        let binding = item.sources.get(source);
        let prev = binding
            .and_then(|b| b.base.as_ref())
            .and_then(|b| b.object.clone());
        let agreed = binding
            .and_then(|b| b.shared_object.clone())
            .or_else(|| prev.clone());
        let shared = item.object.clone();
        let incoming = placement.object.clone();

        let source_edited = incoming != prev;
        let hub_moved = shared != agreed;
        let body_changed = incoming != shared;
        let diverged =
            source_edited && hub_moved && body_changed && incoming.is_some() && shared.is_some();

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
        } else if source_edited && !hub_moved && body_changed {
            item.object = incoming;
            item.conflicted = false;
            item.conflict_object = None;
        }
        // NOTE: else the source carries the shared body already, or is
        // behind the hub, so the shared body stays and the next
        // projection pushes it.
    }

    fn absorb_drop(
        &mut self,
        source: &ReplicaSourceId,
        handle: &ReplicaHandle,
        reason: ReplicaDropReason,
    ) {
        for item in self.items.values_mut() {
            let bound_here = item
                .sources
                .get(source)
                .is_some_and(|binding| &binding.handle == handle);
            if bound_here {
                // NOTE: only a genuine delete propagates. A superseded
                // row is a handle the same batch replaces, and reading
                // it as a delete would push a Remove nobody asked for.
                let genuine = reason == ReplicaDropReason::Deleted;
                item.deleted |= genuine;
                item.sources.remove(source);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use alloc::vec::Vec;

    use crate::{
        change::{ReplicaDropReason, ReplicaWriteOp},
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
                conflict_object: None,
                shared_object: None,
            },
        );
        let item = ReplicaHubItem {
            sort_key: Default::default(),
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
                    conflict_object: None,
                    shared_object: None,
                },
            );
    }

    /// Gives `link` a body on left's base as well as on the item, so it
    /// projects `Full` and reads clean against that source.
    fn hydrate_left(hub: &mut ReplicaHub, link: &str) {
        let item = hub.items.get_mut(&ReplicaLinkId::from(link)).unwrap();
        item.object = Some(ReplicaHash::from("body"));
        item.level = ReplicaLevel::Full;
        for binding in item.sources.values_mut() {
            if let Some(base) = &mut binding.base {
                base.object = Some(ReplicaHash::from("body"));
            }
            binding.shared_object = Some(ReplicaHash::from("body"));
        }
    }

    /// The key the upgrade mints for left's second copy of `m1`.
    fn minted_link() -> ReplicaLinkId {
        ReplicaLinkId::from("dup:m1#l2")
    }

    /// Adds left's second copy of `m1` as the separate item the upgrade
    /// mints for it: its own key, its own handle, its own body.
    fn mint_on_left(hub: &mut ReplicaHub) {
        let mut copy = hub.items.get(&ReplicaLinkId::from("m1")).unwrap().clone();
        copy.sources = [(
            ReplicaSourceId::from("left"),
            ReplicaSourceBinding {
                handle: ReplicaHandle::from("l2"),
                base: Some(base(&["seen"])),
                conflicted: false,
                conflict_revision: None,
                conflict_object: None,
                shared_object: None,
            },
        )]
        .into_iter()
        .collect();
        hub.items.insert(minted_link(), copy);
        hydrate_left(hub, minted_link().as_str());
    }

    #[test]
    fn in_agreement_items_project_clean_without_a_body() {
        // the zero-bodies guardrail: an item both sides agree on
        // projects Clean, at its current level, with no object
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
                    conflict_object: None,
                    shared_object: None,
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
        // left's server adds "seen", so absorbing left's write makes the
        // hub dirty against right's base and right's projection pushes it
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
                    conflict_object: None,
                    shared_object: None,
                },
            );

        // left pulled "seen" from its server
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
        // hydrate the body, still only on left
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
        // no body yet, so the append is not staged and nothing forces a
        // fetch
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
                    conflict_object: None,
                    shared_object: None,
                },
            );

        hub.absorb(
            &ReplicaSourceId::from("left"),
            &[ReplicaWriteOp::DropPlacement {
                collection: "inbox".into(),
                handle: ReplicaHandle::from("l1"),
                reason: ReplicaDropReason::Deleted,
            }],
        );

        let item = hub.items.get(&ReplicaLinkId::from("m1")).expect("kept");
        assert!(!item.sources.contains_key(&ReplicaSourceId::from("left")));
        assert!(item.sources.contains_key(&ReplicaSourceId::from("right")));
    }

    #[test]
    fn a_minted_copy_is_offered_to_a_source_that_holds_neither() {
        // the hub reads no shape into a key, so the second copy of an
        // identity is a member like the first and travels like one
        let mut hub = hub_with_left(&["seen"]);
        hydrate_left(&mut hub, "m1");
        mint_on_left(&mut hub);

        let phone = placements(&hub, "phone");
        assert_eq!(phone.len(), 2, "both copies are offered: {phone:?}");
        for placement in &phone {
            assert_eq!(placement.status, ReplicaStatus::Created);
            assert_eq!(placement.object, Some(ReplicaHash::from("body")));
        }
        assert_eq!(
            placements(&hub, "left").len(),
            2,
            "and the source holding both projects both",
        );
    }

    #[test]
    fn a_drop_of_a_minted_copy_deletes_only_that_copy() {
        // the two copies are two items, so removing one says nothing
        // about the other: the delete propagates for the copy that went
        // and for no other
        let mut hub = hub_with_left(&["seen"]);
        hydrate_left(&mut hub, "m1");
        mint_on_left(&mut hub);
        // right holds both copies too, so left's drop is a removal to
        // propagate rather than the last binding going
        bind_right(&mut hub, &["seen"]);
        hub.items.get_mut(&minted_link()).unwrap().sources.insert(
            ReplicaSourceId::from("right"),
            ReplicaSourceBinding {
                handle: ReplicaHandle::from("r2"),
                base: Some(base(&["seen"])),
                conflicted: false,
                conflict_revision: None,
                conflict_object: None,
                shared_object: None,
            },
        );

        hub.absorb(
            &ReplicaSourceId::from("left"),
            &[ReplicaWriteOp::DropPlacement {
                collection: "inbox".into(),
                handle: ReplicaHandle::from("l2"),
                reason: ReplicaDropReason::Deleted,
            }],
        );

        let minted = hub.items.get(&minted_link()).expect("kept");
        assert!(minted.deleted, "the copy that vanished is gone everywhere");
        let bare = hub.items.get(&ReplicaLinkId::from("m1")).expect("kept");
        assert!(!bare.deleted, "the copy nobody touched is untouched");

        let right = placements(&hub, "right");
        let status = |handle: &str| {
            right
                .iter()
                .find(|p| p.handle.as_str() == handle)
                .expect("right projects both copies")
                .status
        };
        assert_eq!(
            status("r2"),
            ReplicaStatus::Tombstone,
            "right removes the copy that went",
        );
        assert_ne!(
            status("r1"),
            ReplicaStatus::Tombstone,
            "and keeps the one that did not",
        );
    }

    #[test]
    fn a_superseded_row_does_not_delete_the_shared_item() {
        // a placeholder reconciled to its assigned handle, or a spine
        // rebuilt onto a new handle space, drops a row without the item
        // going anywhere
        let mut hub = hub_with_left(&["seen"]);
        bind_right(&mut hub, &["seen"]);

        hub.absorb(
            &ReplicaSourceId::from("left"),
            &[ReplicaWriteOp::DropPlacement {
                collection: "inbox".into(),
                handle: ReplicaHandle::from("l1"),
                reason: ReplicaDropReason::Superseded,
            }],
        );

        let item = hub.items.get(&ReplicaLinkId::from("m1")).expect("kept");
        assert!(!item.deleted, "no delete propagates");
        let right = placements(&hub, "right");
        assert_eq!(right[0].status, ReplicaStatus::Clean, "right is untouched");
    }

    #[test]
    fn a_delete_on_one_source_projects_a_tombstone_on_the_other() {
        let mut hub = hub_with_left(&["seen"]);
        bind_right(&mut hub, &["seen"]);

        // left's server expunged the item, so the engine drops it there
        hub.absorb(
            &ReplicaSourceId::from("left"),
            &[ReplicaWriteOp::DropPlacement {
                collection: "inbox".into(),
                handle: ReplicaHandle::from("l1"),
                reason: ReplicaDropReason::Deleted,
            }],
        );

        let item = hub.items.get(&ReplicaLinkId::from("m1")).expect("kept");
        assert!(item.deleted, "the delete is recorded");
        assert!(!item.sources.contains_key(&ReplicaSourceId::from("left")));

        // right still holds it, so it projects a Tombstone
        let right = placements(&hub, "right");
        assert_eq!(right.len(), 1);
        assert_eq!(right[0].status, ReplicaStatus::Tombstone);
        assert_eq!(right[0].handle.as_str(), "r1");
        assert!(
            right[0].base.is_some(),
            "based, so the engine pushes a remove"
        );
        // left no longer holds it, so it projects nothing rather than a
        // re-copy
        assert!(placements(&hub, "left").is_empty());
    }

    #[test]
    fn a_client_staged_tombstone_upsert_marks_deleted_and_keeps_the_binding() {
        // a Remove, or a Move's source side, stages a Tombstone upsert
        // rather than a drop: absorbing it marks the item deleted while
        // keeping the binding, so the projection pushes the remove
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
                    reason: ReplicaDropReason::Deleted,
                }],
            );
        }

        assert!(hub.items.is_empty(), "no source holds it, so it is pruned");
    }

    #[test]
    fn a_live_upsert_resurrects_a_delete_in_flight() {
        let mut hub = hub_with_left(&["seen"]);
        bind_right(&mut hub, &["seen"]);

        // right deletes it
        hub.absorb(
            &ReplicaSourceId::from("right"),
            &[ReplicaWriteOp::DropPlacement {
                collection: "inbox".into(),
                handle: ReplicaHandle::from("r1"),
                reason: ReplicaDropReason::Deleted,
            }],
        );
        assert!(hub.items.get(&ReplicaLinkId::from("m1")).unwrap().deleted);

        // left's server had edited it, so edit-beats-delete resurrects it
        // as a live upsert rather than pushing the delete
        let mut pulled = ReplicaPlacement {
            sort_key: Default::default(),
            collection: "inbox".into(),
            handle: ReplicaHandle::from("l1"),
            link_id: Some(ReplicaLinkId::from("m1")),
            object: None,
            level: ReplicaLevel::Meta,
            meta: None,
            flags: ReplicaFlags::from_iter(["seen", "flagged"]),
            status: ReplicaStatus::Clean,
            conflict_revision: None,
            conflict_object: None,
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
        // right lacks it now, so it projects a Created copy
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

    /// A hub with one mutable item, body `o0`, held by left and right
    /// and last-synced against it.
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
            conflict_object: None,
            shared_object: Some(ReplicaHash::from("o0")),
        };
        let mut sources = BTreeMap::new();
        sources.insert(ReplicaSourceId::from("left"), based("l1"));
        sources.insert(ReplicaSourceId::from("right"), based("r1"));
        let item = ReplicaHubItem {
            sort_key: Default::default(),
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
            sort_key: Default::default(),
            collection: "inbox".into(),
            handle: ReplicaHandle::from(handle),
            link_id: Some(ReplicaLinkId::from("m1")),
            object: Some(ReplicaHash::from(object)),
            level: ReplicaLevel::Full,
            meta: None,
            flags: ReplicaFlags::default(),
            status: ReplicaStatus::Clean,
            conflict_revision: None,
            conflict_object: None,
            base: Some(ReplicaBase {
                flags: ReplicaFlags::default(),
                revision: Some("r1".into()),
                object: Some(ReplicaHash::from(object)),
            }),
            origin: None,
        })
    }

    /// An offline edit of the shared item from `handle`, which is the
    /// shape a local mutation leaves: the new body against the base last
    /// synced with the source's own remote, which an edit never moves.
    fn edited_upsert(handle: &str, object: &str) -> ReplicaWriteOp {
        let ReplicaWriteOp::UpsertPlacement(mut placement) = content_upsert(handle, object) else {
            unreachable!()
        };

        placement.status = ReplicaStatus::Dirty;
        placement.base = Some(ReplicaBase {
            flags: ReplicaFlags::default(),
            revision: Some("r0".into()),
            object: Some(ReplicaHash::from("o0")),
        });
        ReplicaWriteOp::UpsertPlacement(placement)
    }

    fn item_object(hub: &ReplicaHub) -> Option<ReplicaHash> {
        hub.items
            .get(&ReplicaLinkId::from("m1"))
            .unwrap()
            .object
            .clone()
    }

    #[test]
    fn a_clean_fast_forward_adopts_the_new_body() {
        // only left edited since both agreed on o0, so the body is
        // adopted with no conflict
        let mut hub = content_hub(ReplicaHubConflict::Manual);
        hub.absorb(
            &ReplicaSourceId::from("left"),
            &[content_upsert("l1", "oa")],
        );
        assert_eq!(item_object(&hub), Some(ReplicaHash::from("oa")));
        assert!(
            !hub.items
                .get(&ReplicaLinkId::from("m1"))
                .unwrap()
                .conflicted
        );
    }

    #[test]
    fn a_second_offline_edit_is_not_a_divergence() {
        // one source bound and no second source anywhere: the first edit
        // moves the shared body ahead of the sync base, which is the gap
        // another source folding in leaves, and the second edit arriving
        // over it must not read as the two disagreeing
        let mut hub = content_hub(ReplicaHubConflict::Manual);
        let left = ReplicaSourceId::from("left");
        hub.items
            .get_mut(&ReplicaLinkId::from("m1"))
            .unwrap()
            .sources
            .remove(&ReplicaSourceId::from("right"));

        hub.absorb(&left, &[edited_upsert("l1", "o1")]);
        hub.absorb(&left, &[edited_upsert("l1", "o2")]);

        let item = hub.items.get(&ReplicaLinkId::from("m1")).unwrap();
        assert_eq!(
            item.object,
            Some(ReplicaHash::from("o2")),
            "the second edit is the shared body"
        );
        assert!(!item.conflicted, "a source cannot disagree with itself");
        assert_eq!(item.conflict_object, None, "so nothing diverges from it");
    }

    #[test]
    fn a_divergence_between_unpushed_edits_still_conflicts() {
        // the two edits leave the same gap between base and shared body
        // that a source's own second edit does, and telling one from the
        // other is the whole point: this one is two sources disagreeing
        let mut hub = content_hub(ReplicaHubConflict::Manual);
        hub.absorb(&ReplicaSourceId::from("left"), &[edited_upsert("l1", "oa")]);
        hub.absorb(
            &ReplicaSourceId::from("right"),
            &[edited_upsert("r1", "ob")],
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
            "and the diverging one preserved"
        );
    }

    #[test]
    fn divergent_content_conflicts_and_preserves_both_manual() {
        // left edited to oa, then right to ob against the old o0 base:
        // both moved, so Manual flags it and records ob, keeping oa
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

    /// A conflicted upsert for the shared item from `handle`, carrying
    /// the local body, the remote revision the merge observed and the
    /// diverging body an upgrade supplied for it.
    fn conflicted_upsert(handle: &str, object: &str, revision: &str) -> ReplicaWriteOp {
        ReplicaWriteOp::UpsertPlacement(ReplicaPlacement {
            sort_key: Default::default(),
            collection: "inbox".into(),
            handle: ReplicaHandle::from(handle),
            link_id: Some(ReplicaLinkId::from("m1")),
            object: Some(ReplicaHash::from(object)),
            level: ReplicaLevel::Full,
            meta: None,
            flags: ReplicaFlags::default(),
            status: ReplicaStatus::Conflict,
            conflict_revision: Some(revision.into()),
            conflict_object: Some(ReplicaHash::from("o-remote")),
            base: Some(ReplicaBase {
                flags: ReplicaFlags::default(),
                revision: Some("r0".into()),
                object: Some(ReplicaHash::from("o0")),
            }),
            origin: None,
        })
    }

    #[test]
    fn a_conflicted_placement_round_trips_with_its_diverging_body() {
        // the merge marks a placement Conflict and records the remote
        // revision it saw and the body at it, and all three must come
        // back out: read back as Dirty, the engine would re-derive the
        // rejected push and re-conflict on every run without converging,
        // and a lost body would send the resolver to the network
        let mut hub = content_hub(ReplicaHubConflict::Manual);
        hub.absorb(
            &ReplicaSourceId::from("left"),
            &[conflicted_upsert("l1", "o-local", "r-remote")],
        );

        let projected = placements(&hub, "left");
        assert_eq!(projected.len(), 1);
        assert_eq!(projected[0].status, ReplicaStatus::Conflict);
        assert_eq!(
            projected[0].object,
            Some(ReplicaHash::from("o-local")),
            "the local side of the divergence is the shared body"
        );
        assert_eq!(
            projected[0].conflict_revision.as_deref(),
            Some("r-remote"),
            "the observed remote revision is what a resolver merges against"
        );
        assert_eq!(
            projected[0].conflict_object,
            Some(ReplicaHash::from("o-remote")),
            "the diverging body comes back with the revision it describes"
        );
    }

    #[test]
    fn a_conflict_outranks_the_base_comparison() {
        // a matching base would otherwise project Clean, silently losing
        // the conflict, and a diverging one Dirty, re-deriving the
        // rejected push
        let mut hub = content_hub(ReplicaHubConflict::Manual);
        hub.absorb(
            &ReplicaSourceId::from("left"),
            &[conflicted_upsert("l1", "o0", "r-remote")],
        );

        let binding =
            hub.items[&ReplicaLinkId::from("m1")].sources[&ReplicaSourceId::from("left")].clone();
        assert!(binding.conflicted);
        // the base equals the shared content, so the Clean branch would
        // win without the conflict check
        assert_eq!(
            binding.base.as_ref().and_then(|b| b.object.clone()),
            hub.items[&ReplicaLinkId::from("m1")].object
        );
        assert_eq!(placements(&hub, "left")[0].status, ReplicaStatus::Conflict);
    }

    #[test]
    fn resolving_the_conflict_with_an_edit_clears_it() {
        // the consumer's edit arrives as an ordinary upsert, and any
        // status but Conflict clears the binding
        let mut hub = content_hub(ReplicaHubConflict::Manual);
        let left = ReplicaSourceId::from("left");
        hub.absorb(&left, &[conflicted_upsert("l1", "o-local", "r-remote")]);
        assert_eq!(placements(&hub, "left")[0].status, ReplicaStatus::Conflict);

        hub.absorb(&left, &[edited_upsert("l1", "o-merged")]);

        let item = hub.items[&ReplicaLinkId::from("m1")].clone();
        assert_eq!(
            item.object,
            Some(ReplicaHash::from("o-merged")),
            "the merged body is the shared body, or the next run pushes the unmerged one"
        );

        let binding = item.sources[&left].clone();
        assert!(!binding.conflicted);
        assert_eq!(
            binding.conflict_revision, None,
            "a resolved binding must not carry a stale revision forward"
        );
        assert_eq!(
            binding.conflict_object, None,
            "nor the body that revision named"
        );
        assert_ne!(placements(&hub, "left")[0].status, ReplicaStatus::Conflict);
    }

    #[test]
    fn the_two_conflict_axes_stay_independent() {
        // a source-vs-its-own-remote conflict and a cross-source one are
        // different facts, and neither may leak into the other's flag
        let mut hub = content_hub(ReplicaHubConflict::Manual);
        let link = ReplicaLinkId::from("m1");

        // left conflicts with its own server: the binding is marked, the
        // item's cross-source flag is not
        hub.absorb(
            &ReplicaSourceId::from("left"),
            &[conflicted_upsert("l1", "o-local", "r-remote")],
        );
        assert!(hub.items[&link].sources[&ReplicaSourceId::from("left")].conflicted);
        assert!(
            !hub.items[&link].conflicted,
            "a per-source conflict is not a cross-source one"
        );

        // right's binding is untouched by left's conflict
        assert!(!hub.items[&link].sources[&ReplicaSourceId::from("right")].conflicted);
        assert_eq!(placements(&hub, "right")[0].status, ReplicaStatus::Dirty);

        // a genuine cross-source divergence: the item is flagged, and
        // right's own binding is not
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
        // the tombstone path takes the same binding constructor, and a
        // staged delete must not inherit a stale conflict
        let mut hub = content_hub(ReplicaHubConflict::Manual);
        let left = ReplicaSourceId::from("left");
        hub.absorb(&left, &[conflicted_upsert("l1", "o-local", "r-remote")]);

        let mut tombstone = match content_upsert("l1", "o0") {
            ReplicaWriteOp::UpsertPlacement(p) => p,
            _ => unreachable!(),
        };
        tombstone.status = ReplicaStatus::Tombstone;
        hub.absorb(&left, &[ReplicaWriteOp::UpsertPlacement(tombstone)]);

        let item = hub.items[&ReplicaLinkId::from("m1")].clone();
        assert_eq!(
            item.object,
            Some(ReplicaHash::from("o-local")),
            "a staged delete adopts no content"
        );

        let binding = item.sources[&left].clone();
        assert!(!binding.conflicted);
        assert_eq!(binding.conflict_revision, None);
        assert_eq!(binding.conflict_object, None);
    }

    #[test]
    fn a_tombstone_does_not_move_the_agreement_point() {
        // a staged delete adopts no body, so it says nothing about what
        // its source agreed with: reading it as agreement would have
        // right's later edit fast-forward over left's body
        let mut hub = content_hub(ReplicaHubConflict::Manual);
        let right = ReplicaSourceId::from("right");
        hub.absorb(&ReplicaSourceId::from("left"), &[edited_upsert("l1", "oa")]);

        let ReplicaWriteOp::UpsertPlacement(mut tombstone) = edited_upsert("r1", "o0") else {
            unreachable!()
        };
        tombstone.status = ReplicaStatus::Tombstone;
        hub.absorb(&right, &[ReplicaWriteOp::UpsertPlacement(tombstone)]);
        hub.absorb(&right, &[edited_upsert("r1", "ob")]);

        let item = hub.items.get(&ReplicaLinkId::from("m1")).unwrap();
        assert!(item.conflicted, "right never saw left's body");
        assert_eq!(item.object, Some(ReplicaHash::from("oa")));
        assert_eq!(item.conflict_object, Some(ReplicaHash::from("ob")));
    }
}

#[cfg(test)]
mod sort_key_tests {

    use crate::{
        change::ReplicaWriteOp,
        hub::*,
        placement::{ReplicaFlags, ReplicaHandle, ReplicaLevel, ReplicaLinkId, ReplicaPlacement},
    };

    fn upsert(source: &str, key: &str) -> ReplicaWriteOp {
        let _ = source;
        ReplicaWriteOp::UpsertPlacement(ReplicaPlacement {
            collection: ReplicaCollectionId::from("inbox"),
            handle: ReplicaHandle::from("1"),
            link_id: Some(ReplicaLinkId::from("mid:a@host")),
            object: None,
            level: ReplicaLevel::Meta,
            meta: None,
            sort_key: ReplicaSortKey::from(key),
            flags: ReplicaFlags::default(),
            status: ReplicaStatus::Clean,
            conflict_revision: None,
            conflict_object: None,
            base: None,
            origin: None,
        })
    }

    #[test]
    fn a_sort_key_round_trips_through_the_hub() {
        // a key absorbed from one source has to come back out when that
        // source is projected, or the storage below reads it as unknown
        // on every load
        let mut hub = ReplicaHub::default();
        let left = ReplicaSourceId::from("left");

        hub.absorb(&left, &[upsert("left", "2026-08-01T10:00:00Z")]);

        let projected = hub.project(&ReplicaCollectionId::from("inbox"), &left);
        assert_eq!(projected.len(), 1);
        assert_eq!(projected[0].sort_key.0, "2026-08-01T10:00:00Z");
    }

    #[test]
    fn an_unknown_key_does_not_erase_a_known_one() {
        // a second source that has only probed the item carries no key,
        // and adopting it would un-sort an item the first source had
        // already placed
        let mut hub = ReplicaHub::default();
        let left = ReplicaSourceId::from("left");
        let right = ReplicaSourceId::from("right");

        hub.absorb(&left, &[upsert("left", "2026-08-01T10:00:00Z")]);
        hub.absorb(&right, &[upsert("right", "")]);

        let projected = hub.project(&ReplicaCollectionId::from("inbox"), &left);
        assert_eq!(projected[0].sort_key.0, "2026-08-01T10:00:00Z");
    }

    #[test]
    fn a_later_derivation_replaces_an_earlier_one() {
        // a `Full` fetch knows more than an envelope did, so a real key
        // corrects a real key; only unknown is inert
        let mut hub = ReplicaHub::default();
        let left = ReplicaSourceId::from("left");

        hub.absorb(&left, &[upsert("left", "2026-08-01T10:00:00Z")]);
        hub.absorb(&left, &[upsert("left", "2026-08-02T09:00:00Z")]);

        let projected = hub.project(&ReplicaCollectionId::from("inbox"), &left);
        assert_eq!(projected[0].sort_key.0, "2026-08-02T09:00:00Z");
    }
}

/// Flags carry the same unknown state a sort key does (spec §13: the
/// store's `flags` column is `NULL` until something reads them, distinct
/// from a known-empty `'[]'`), and the hub owes it the same rule.
#[cfg(test)]
mod flags_tests {

    use crate::{
        change::ReplicaWriteOp,
        hub::*,
        placement::{ReplicaFlags, ReplicaHandle, ReplicaLevel, ReplicaLinkId, ReplicaPlacement},
    };

    fn upsert(flags: ReplicaFlags) -> ReplicaWriteOp {
        ReplicaWriteOp::UpsertPlacement(ReplicaPlacement {
            collection: ReplicaCollectionId::from("inbox"),
            handle: ReplicaHandle::from("1"),
            link_id: Some(ReplicaLinkId::from("mid:a@host")),
            object: None,
            level: ReplicaLevel::Meta,
            meta: None,
            sort_key: ReplicaSortKey::default(),
            flags,
            status: ReplicaStatus::Clean,
            conflict_revision: None,
            conflict_object: None,
            base: None,
            origin: None,
        })
    }

    #[test]
    fn an_unknown_set_does_not_erase_a_known_one() {
        // a second source that has only probed the item must not clear
        // the markers the first one read
        let mut hub = ReplicaHub::default();
        let left = ReplicaSourceId::from("left");
        let right = ReplicaSourceId::from("right");

        hub.absorb(&left, &[upsert(ReplicaFlags::from_iter(["seen"]))]);
        hub.absorb(&right, &[upsert(ReplicaFlags::Unknown)]);

        let projected = hub.project(&ReplicaCollectionId::from("inbox"), &left);
        assert!(projected[0].flags.contains("seen"));
    }

    #[test]
    fn a_known_set_replaces_an_unknown_one() {
        // only unknown is inert: a deliberate clearing is a real set and
        // corrects another
        let mut hub = ReplicaHub::default();
        let left = ReplicaSourceId::from("left");

        hub.absorb(&left, &[upsert(ReplicaFlags::Unknown)]);
        hub.absorb(&left, &[upsert(ReplicaFlags::from_iter(["seen"]))]);
        let projected = hub.project(&ReplicaCollectionId::from("inbox"), &left);
        assert!(projected[0].flags.contains("seen"));

        hub.absorb(&left, &[upsert(ReplicaFlags::default())]);
        let projected = hub.project(&ReplicaCollectionId::from("inbox"), &left);
        assert_eq!(projected[0].flags, ReplicaFlags::default());
    }
}

#[cfg(test)]
mod stored_level_tests {

    use crate::{
        change::ReplicaWriteOp,
        hub::*,
        object::ReplicaHash,
        placement::{ReplicaFlags, ReplicaHandle, ReplicaLevel, ReplicaLinkId, ReplicaPlacement},
    };

    /// One source's placement of the same item, at the stated level and body.
    fn upsert(level: ReplicaLevel, object: Option<&str>, base: Option<&str>) -> ReplicaWriteOp {
        ReplicaWriteOp::UpsertPlacement(ReplicaPlacement {
            collection: ReplicaCollectionId::from("contacts"),
            handle: ReplicaHandle::from("card-1.vcf"),
            link_id: Some(ReplicaLinkId::from("uid:card-1")),
            object: object.map(ReplicaHash::from),
            level,
            meta: Some(ReplicaMeta(String::from("{\"v\":1}"))),
            sort_key: ReplicaSortKey::default(),
            flags: ReplicaFlags::default(),
            status: ReplicaStatus::Clean,
            conflict_revision: None,
            conflict_object: None,
            base: Some(ReplicaBase {
                flags: ReplicaFlags::default(),
                revision: None,
                object: base.map(ReplicaHash::from),
            }),
            origin: None,
        })
    }

    #[test]
    fn a_refreshed_item_stops_claiming_the_body_it_lost() {
        // the merge dropped the stale body, so the item is summarised
        // but no longer stored
        let mut hub = ReplicaHub::default();
        let left = ReplicaSourceId::from("left");

        hub.absorb(
            &left,
            &[upsert(ReplicaLevel::Full, Some("body1"), Some("body1"))],
        );
        hub.absorb(&left, &[upsert(ReplicaLevel::Probed, None, None)]);

        let item = &hub.items[&ReplicaLinkId::from("uid:card-1")];
        assert_eq!(item.object, None, "the stale body is gone");
        assert_eq!(
            item.level,
            ReplicaLevel::Meta,
            "so the level cannot be Full"
        );

        let projected = hub.project(&ReplicaCollectionId::from("contacts"), &left);
        assert_eq!(projected[0].level, ReplicaLevel::Meta);
    }

    #[test]
    fn a_body_less_item_stored_as_full_projects_below_it() {
        // an upgrade reads the projection, so this is what heals a store
        // written before the rule
        let mut hub = ReplicaHub::default();
        let left = ReplicaSourceId::from("left");

        hub.absorb(&left, &[upsert(ReplicaLevel::Full, Some("body1"), None)]);
        let item = hub
            .items
            .get_mut(&ReplicaLinkId::from("uid:card-1"))
            .expect("the absorbed item");
        item.object = None;
        item.level = ReplicaLevel::Full;

        let projected = hub.project(&ReplicaCollectionId::from("contacts"), &left);
        assert_eq!(projected[0].level, ReplicaLevel::Meta);
    }

    #[test]
    fn a_stored_body_keeps_the_level_it_reached() {
        // the rule is the body's absence and nothing else
        let mut hub = ReplicaHub::default();
        let left = ReplicaSourceId::from("left");

        hub.absorb(&left, &[upsert(ReplicaLevel::Full, Some("body1"), None)]);

        let item = &hub.items[&ReplicaLinkId::from("uid:card-1")];
        assert_eq!(item.level, ReplicaLevel::Full);
        assert_eq!(item.stored_level(), ReplicaLevel::Full);
    }
}
