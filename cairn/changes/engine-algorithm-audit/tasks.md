---
cairn: tasks
change: engine-algorithm-audit
---

# Tasks

Triage: each accepted item became its own change with its own delta, named below. Nothing was landed by this change itself.

## Decide

- [x] `Move`: server-side move, or copy and remove? **Both**, recognising each other through the link id: the create delivers by copying from its origin, the remove relocates only while the destination does not already hold the identity. Dropping either half loses data in one of the two sync orders.
- [x] What `DropPlacement` means, and whether it carries a reason. **`ReplicaDropReason::{Deleted, Superseded}`**; only `Deleted` propagates through the hub.
- [x] Whether the write batch is order-significant, or whether `rekey` stops depending on it. **Both** (`write-batch-order`). `rekey` drops only the old handles no upsert of the same batch writes, which closed two collisions: a reused handle, and a resurrected edit under the handle it already had. The contract now states that a batch is applied in order and may not be grouped by op kind, because a sync legitimately writes a placement whose ambiguity cleared and then drops the same handle it reads as vanished.
- [x] Whether a link id is unique per collection and source. **No**, and one a source holds twice is frozen rather than guessed (`duplicate-link-id-freeze`).
- [x] Whether `enumerated` becomes a real invariant or the field goes. **The field went**, with `ReplicaCollection`.

## Correctness

- [x] Fix the `Move` double delivery, and order the target add before the source remove.
- [x] Keep a local delete pending on a read-only source under a delta enumerate, or force the next run full. Reconciling `push = false` with `rights.remove = false` landed in `delete-disposition`.
- [~] Key hub bindings by source and handle so two placements sharing a link id survive. **Rejected on merit.** Freezing the identity and reporting it until a human resolves it is simpler and truer than making the engine hold an ambiguity it cannot resolve: 1:N bindings would spread a guess across every source. Bindings stay 1:1 (`duplicate-link-id-freeze`).
- [x] Give a `KeepBoth` duplicate a link id and a unique synthetic handle, both derived from the forked body.
- [x] Stop `absorb_drop` propagating a local-only drop as a delete.
- [x] Revisit a `Full` placement holding no object, and a `Meta` one holding no meta.
- [x] Close whether `pull_flags`'s fabricated base is the base-presence bug io-pimdir sees. **It is not, and the fabrication is not a lie.** `ReplicaPlacement::staged_edit` is deliberately status-free and reads a base holding no object exactly as it reads no base at all, so a body nothing has confirmed the remote holds reads as unsynced either way. That is true rather than wrong: the flag axis has no basis for claiming the remote holds a body it never reported. The 0.4.2 "dirty forever" shape was the upgrade dedup branch skipping its rebase, which is a different write.
- [ ] **The residual that analysis did surface**: a never-based placement holding a body, present on both sides of an *immutable* backend, falls through `reconcile_content` (no revision, so no content signal), and the flag axis then lands it `Clean` with a base holding no object. The body is neither pushed (a content push needs `Dirty`) nor forgotten, so it is stranded. The mutable twin of this is already a create-collision conflict. Reachable only if a consumer mints a provisional handle that collides with a real one; wants a reproduction from a consumer before the merge is changed, since widening the collision rule would turn benign cases into conflicts a user has to resolve.

## Shape

- [x] Scope on `WantsLoad`: all, a handle set, or a link. `mutate` and `upgrade` stopped loading collections.
- [x] Merge-join the full sync over sorted iterators; borrow the placement and own one copy at most per handle (`merge-join`).
- [ ] Index the hub from source and handle to link, so a drop seeks. Mitigated rather than fixed: a drop now scans the *scoped* hub a storage loads for its batch. Performance only.
- [x] Reconcile flags even when a content push is derived. *Keying the pending maps by handle and kind is still open*: one handle yields at most one change per run, because a push result names a handle. Now cheaper than it was, since every change carries a `ReplicaChangeKey` a result could name instead.
- [x] Chunk pushes with their recording writes; derive an idempotency key for every change, not only adds (`chunked-pushes`).
- [x] Make the disposition of a local delete an explicit option rather than an emergent one (`delete-disposition`). `ReplicaDeletePolicy { Revert, Keep }`, `Revert` by default, consulted wherever a delete cannot go, so `ReplicaPushRights::none()` and `push = false` agree on it.

## Compaction

- [x] Delete `ReplicaCollection`.
- [~] Delete `open.rs`. **Rejected**: pimalaya-linux drives `ReplicaOpen` as its generic read path in four places.
- [~] Split `ReplicaStatus` into membership, flag-dirty and content state. **Withdrawn on merit**, not deferred: the six re-derived edit predicates are now one `staged_edit`, the tombstone bug was fixed where it lived, and `Ambiguous` is a variant precisely so the compiler forces every rule to say what it does with an unresolvable identity. Splitting the enum into axes would give that exhaustiveness up.
- [x] One macro for the string newtypes (`replica_id!`); one argument error enum; one hub placement builder (`engine-compaction`).
- [x] Drop `may_push`; drop the unreachable branch in `stage_conflict_dup`; error on resuming a completed coroutine (`engine-compaction`); count only matched pushes.
- [~] Rename `Remove { to: None }` to what it means. **Closed as stated, without the rename**: the destination is optional because the consumer drops it when the move's other half already delivered, which only works while a delete and a relocation are one operation. The trash-routing policy that did not belong on the seam is out of the doc.

## Tests

- [x] A move end to end, in both sync orders, linked and never-fetched (`tests/membership.rs`).
- [x] A local delete on a read-only source under a delta enumerate.
- [x] Two placements sharing a link id in one collection (`tests/duplicate_link_id.rs`).
- [x] **The hub driven by the real sync engine** (`hub-sync-harness`, `tests/hub.rs`). Eight scenarios over two sources and one shared store. It found that a hub-backed store must own the rows the hub cannot key (io-pimdir's residual list is required, not a bolt-on), that mirroring is a sync plus an upgrade, and that a reverted delete reads as add-beats-delete across sources.
- [ ] `KeepBoth`, `PreferLocal` and `PreferRemote` end to end, and under crash injection. The property model only runs `Manual`.
- [ ] A rekey while a hub is bound. The overlapping handle space half landed with `write-batch-order`; the hub half now has `tests/hub.rs` to be written against.
- [x] A push result set that is short, out of order, duplicated, or names an unknown handle (`hub-sync-harness`, in `sync.rs`): an unreported push stays dirty and counts as neither pushed nor rejected, and results are matched by handle, so a duplicate or an unknown handle changes nothing.
- [x] Rights combinations under the hub, such as one source refusing removes while another deletes (`hub-sync-harness`), under both delete policies.
- [x] A write batch applied in a different order than emitted: **contractual** (`write-batch-order`). It is applied in order, and a rebuild no longer emits a pair that depends on it.
