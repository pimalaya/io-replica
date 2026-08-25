---
cairn: log
date: 2026-08-25
change: write-batch-order
---

# A rebuilt spine stops betting on the order it is applied in

The audit asked whether the write batch is order-significant and the answer came back half-done: marking a renumbering's drops `Superseded` stopped a hub reading it as a mass delete, but `rekey` still dropped *every* old handle before upserting the new spine, and two of those drops collide with the batch's own upserts.

A reused handle is the obvious one, and it is the common case rather than the exotic one: a server renumbering a mailbox hands out UIDs from the same range, so the batch holds a drop and an upsert of the same key. The second needs no overlap at all and had been there since staged edits learned to survive a bump: an unmatched staged edit is resurrected as a pending create **under the handle it already had**, which the top of the same batch dropped. Every rekey that resurrects an edit emitted the pair.

Both were correct, and correct only because storages happen to iterate the list. The contract promised atomicity and nothing else, and neither op said anything about the other, so the reasoning lived nowhere.

`rekey` now tracks the handles its upserts write and drops only the rest. The drops moved to the end of the batch in the process, which is exactly the point: it no longer matters where they are.

## What the contract says now

Not "the batch is a set". The engine still emits an upsert and a drop for one handle in a sync: `thaw` writes a placement whose ambiguity has cleared, and the merge in the same run can then read that handle as vanished from a complete snapshot and drop it. The drop is the answer, and the pair is meaningful.

So `ReplicaStorage::write` applies the ops **in order**, and a storage may not group them by op kind, which is the tempting sqlite optimisation (one prepared statement per kind). What the rebuild fix buys is not the contract, it is that a rebuild no longer *needs* it: a collision between two ops far apart in a long list is the kind that survives review.

## Verification

- 205 tests green, `cargo clippy --all-targets` clean, `cargo fmt`.
- Two tests, both written against the old code and watched fail: a reused handle and a resurrected edit are each written once, with no drop of the same handle in the batch. They assert the invariant on the write list itself, so they hold whatever a storage does with it.
- `report.dropped` is unchanged and still means what it meant: the pending state lost with the old handle space. A row whose handle another item reuses is still lost, it is overwritten rather than dropped.

Capabilities moved: `storage`.
