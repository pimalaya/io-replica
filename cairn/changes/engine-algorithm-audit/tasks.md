---
cairn: tasks
change: engine-algorithm-audit
---

# Tasks

Triage first: each accepted item becomes its own change with its own delta. Nothing below is landed by this change.

## Decide

- [ ] `Move`: server-side move, or copy and remove? Both are wired.
- [ ] What `DropPlacement` means, and whether it carries a reason.
- [ ] Whether the write batch is order-significant, or whether `rekey` stops depending on it.
- [ ] Whether a link id is unique per collection and source.
- [ ] Whether `enumerated` becomes a real invariant or the field goes.

## Correctness

- [ ] Fix the `Move` double delivery, and order the target add before the source remove.
- [ ] Keep a local delete pending on a read-only source under a delta enumerate, or force the next run full. Reconcile `push = false` with `rights.remove = false`.
- [ ] Key hub bindings by source and handle so two placements sharing a link id survive.
- [ ] Give a `KeepBoth` duplicate a link id and a unique synthetic handle.
- [ ] Stop `absorb_drop` propagating a local-only drop as a delete.
- [ ] Revisit a `Full` placement holding no object, and a `Meta` one holding no meta.
- [ ] Close whether `pull_flags`'s fabricated base is the base-presence bug io-pimdir sees.

## Shape

- [ ] Scope on `WantsLoad`: all, a handle set, or a link. `mutate` and `upgrade` stop loading collections.
- [ ] Merge-join the full sync over sorted iterators; borrow the placement and own one copy at most per handle.
- [ ] Index the hub from source and handle to link, so a drop seeks.
- [ ] Reconcile flags even when a content push is derived; key the pending maps by handle and kind.
- [ ] Chunk pushes with their recording writes; derive an idempotency key for every change, not only adds.
- [ ] Make the disposition of a local delete an explicit option rather than an emergent one.

## Compaction

- [ ] Delete `ReplicaCollection`.
- [ ] Delete `open.rs`; the client calls the storage directly.
- [ ] Split `ReplicaStatus` into membership, flag-dirty and content state; drop the seven special cases and the six re-derived edit predicates.
- [ ] One macro for the string newtypes; one argument error enum; one hub placement builder.
- [ ] Drop `may_push`; drop the unreachable branch in `stage_conflict_dup`; error on resuming a completed coroutine; count only matched pushes.
- [ ] Rename `Remove { to: None }` to what it means, and leave trash routing to the consumer.

## Tests

- [ ] The nine adversarial scenarios listed in the proposal, each verified failing first where it encodes a bug.
