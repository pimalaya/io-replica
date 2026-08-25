---
cairn: log
change: freeze-is-one-item-wide
date: 2026-08-25
---

# The freeze is one item wide, now on purpose

`duplicate-link-id-freeze` decided an identity a source holds twice derives nothing until the source resolves it. The cost is that the item stops syncing, and for a duplicate nobody removes it stops for good. What makes that acceptable is the scope, and the scope was true, untested and unstated.

## What was already true

Checked on every path before writing anything:

- **sync**: `merge` returns `None` for an ambiguous candidate and the join walks on, so the rest of the collection reconciles in the same run.
- **upgrade**: every member of the batch is upgraded and written; the ambiguous ones are marked by `freeze` afterwards.
- **hub**: an ambiguous item does not propagate a delete across sources, which is the data guard rather than a stop.
- **the queue drain** (io-pimdir): `ReplicaMutateError::Ambiguous` comes back through `stage_action` as a park reason, so the row is parked with its error and the drain continues to the next action.
- **the report** (neverest): named per side, per collection, with every handle, re-derived every run, and worded so the user knows the engine cannot tell the copies apart rather than that their mailbox is broken.

So the policy was already "report it, skip it, carry on". Nothing was blocking.

## What landed

- **The scope is a requirement**, folded into "An ambiguous identity derives nothing": the freeze is one item wide and halts nothing, a run meeting one completes, and a mutation against a frozen placement is refused as that mutation rather than as the run.

  The reasoning is worth having beside it, because it is what bounds the rule: deriving nothing is safe only while the nothing is scoped. Applied a batch or a collection wide, the same rule strands a mailbox on one double delivery, which is a worse outcome than the mispairing the freeze exists to prevent, and one the user cannot clear.

- **A test that pins it.** Every existing test in `tests/duplicate_link_id.rs` is about the frozen item; none said anything about its neighbours. `a_frozen_item_does_not_stop_the_collection` seeds an ordinary member beside the twins, changes flags on both remotely, and stages a local edit on the neighbour: the neighbour pulls, the neighbour pushes, the twins stay frozen.

No behaviour changed. The point is that none can change by accident now.

## Verification

217 tests green, `cargo clippy --all-targets --all-features` clean, `cargo fmt`.

Capabilities moved: `sync`.
