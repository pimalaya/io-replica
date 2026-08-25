---
cairn: change
id: freeze-is-one-item-wide
status: landed
created: 2026-08-25
---

# The blast radius of a freeze was true and unstated

## Why

`duplicate-link-id-freeze` decided that an identity a source holds twice derives nothing, in either direction, until the source resolves it. That is the right call and it has a cost: the item stops syncing, and for a duplicate nobody removes it stops for good.

What makes that cost acceptable is that it is **one item wide**. The rest of the collection reconciles normally, a queued mutation against a frozen placement parks rather than stopping the drain, and the run completes with a warning the user can act on. Every one of those is true today, and none of it was written down or tested.

That is the wrong way round for a property this load-bearing. A rule that derives nothing is safe only while the nothing is scoped; the same rule applied a batch or a collection wide would strand a mailbox on one double delivery, which is worse than the mispairing the freeze exists to prevent, and worse in a way the user cannot clear. Nothing stopped a later change from widening it by accident, because nothing said it must not.

## What

- A requirement: the freeze is one item wide and halts nothing, with the reasoning about why scoping is what makes the rule acceptable.
- A test that pins it: a frozen identity beside an ordinary member, remote changes on both axes and a local edit, asserting the neighbour pulls and pushes while the twins stay frozen.

No behaviour changes. The point is that none is possible by accident now.
