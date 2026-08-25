---
cairn: change
id: write-batch-order
status: landed
created: 2026-08-25
---

# A rebuilt spine stops betting on the order it is applied in

## Why

`engine-algorithm-audit` asked whether a write batch is order-significant, and left the question half answered: marking a renumbering's drops `Superseded` stopped a hub reading it as a mass delete, but `rekey` still emitted a drop for *every* old handle before upserting the new spine. Two of those collide with the batch's own upserts:

- **A reused handle.** A new handle space commonly overlaps the old one, since a server renumbering a mailbox hands out UIDs from the same range. The batch then holds `DropPlacement(h)` and `UpsertPlacement(h)` for one key.
- **A resurrected edit**, which needs no overlap at all. An unmatched staged edit survives as a pending create under *the handle it already had*, and that handle was dropped at the top of the same batch. Every rekey that resurrects an edit emits the pair.

Both are correct only while the storage applies the list in order, which the contract has never said, and the reasoning that they are safe lives in neither op.

## What

- Emit a drop only for the old handles no upsert of the same batch writes.
- State the contract the engine does still depend on: a batch is applied in order, and a storage may not group it by op kind. A sync legitimately emits an upsert and a drop for one handle (an ambiguity cleared, then the same handle read as vanished), and the drop is the answer.

## Scope / non-goals

- No API change. `report.dropped` still counts the pending state lost with the old handle space, which is what it meant: a row whose handle another item reuses is still lost, it is simply overwritten rather than dropped.
