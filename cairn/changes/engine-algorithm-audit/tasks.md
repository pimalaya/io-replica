---
cairn: tasks
change: engine-algorithm-audit
---

# Tasks

Triage: each accepted item became its own change with its own delta, named below. Nothing was landed by this change itself.

## Decide

- [x] `Move`: server-side move, or copy and remove? **Both**, recognising each other through the link id: the create delivers by copying from its origin, the remove relocates only while the destination does not already hold the identity. Dropping either half loses data in one of the two sync orders.
- [x] What `DropPlacement` means, and whether it carries a reason. **`ReplicaDropReason::{Deleted, Superseded}`**; only `Deleted` propagates through the hub.
- [ ] Whether the write batch is order-significant, or whether `rekey` stops depending on it. **Half done, and the half that is left is a live bug.** Marking a renumbering's drops `Superseded` stopped the hub reading it as a mass delete, but `rekey` still emits a drop for *every* old handle before upserting the new spine (rekey.rs, `writes`), so a new handle space overlapping the old one puts `DropPlacement(h)` and `UpsertPlacement(h)` for the same key in one batch and the end state depends on which the storage applies last. Overlap is the common case after a UIDVALIDITY bump. Fix: drop only the old handles the new spine does not reuse. Then the contract can state that a batch is a set applied atomically, with references resolving against the whole batch.
- [x] Whether a link id is unique per collection and source. **No**, and one a source holds twice is frozen rather than guessed (`duplicate-link-id-freeze`).
- [x] Whether `enumerated` becomes a real invariant or the field goes. **The field went**, with `ReplicaCollection`.

## Correctness

- [x] Fix the `Move` double delivery, and order the target add before the source remove.
- [x] Keep a local delete pending on a read-only source under a delta enumerate, or force the next run full. *Reconciling `push = false` with `rights.remove = false` is still open*, below.
- [~] Key hub bindings by source and handle so two placements sharing a link id survive. **Rejected on merit.** Freezing the identity and reporting it until a human resolves it is simpler and truer than making the engine hold an ambiguity it cannot resolve: 1:N bindings would spread a guess across every source. Bindings stay 1:1 (`duplicate-link-id-freeze`).
- [x] Give a `KeepBoth` duplicate a link id and a unique synthetic handle, both derived from the forked body.
- [x] Stop `absorb_drop` propagating a local-only drop as a delete.
- [x] Revisit a `Full` placement holding no object, and a `Meta` one holding no meta.
- [ ] Close whether `pull_flags`'s fabricated base is the base-presence bug io-pimdir sees. Needs a reproduction from the io-pimdir end; deferred until the pimdir work is done.

## Shape

- [x] Scope on `WantsLoad`: all, a handle set, or a link. `mutate` and `upgrade` stopped loading collections.
- [x] Merge-join the full sync over sorted iterators; borrow the placement and own one copy at most per handle (`merge-join`).
- [ ] Index the hub from source and handle to link, so a drop seeks. Mitigated rather than fixed: a drop now scans the *scoped* hub a storage loads for its batch. Performance only.
- [x] Reconcile flags even when a content push is derived. *Keying the pending maps by handle and kind is still open*: one handle yields at most one change per run, because a push result names a handle. Now cheaper than it was, since every change carries a `ReplicaChangeKey` a result could name instead.
- [x] Chunk pushes with their recording writes; derive an idempotency key for every change, not only adds (`chunked-pushes`).
- [ ] Make the disposition of a local delete an explicit option rather than an emergent one. `push = false` reverts the delete, `rights.remove = false` keeps it pending, so `ReplicaPushRights::none()` is not `push = false` and nothing says so.

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
- [ ] **The hub driven by the real sync engine.** Nothing in `tests/` touches the hub: its tests are project and absorb units over hand-built writes, so project, sync, absorb, project convergence has never run. The largest remaining unknown in the crate.
- [ ] `KeepBoth`, `PreferLocal` and `PreferRemote` end to end, and under crash injection. The property model only runs `Manual`.
- [ ] A rekey while a hub is bound, and a rekey whose new handle space overlaps the old one.
- [ ] A push result set that is short, out of order, duplicated, or names an unknown handle. The rule that an unreported push stays pending has no test, and the chunked drain now rests on it.
- [ ] Rights combinations under the hub, such as one source refusing removes while another deletes.
- [ ] A write batch applied in a different order than emitted, which should either break loudly or become contractual.
