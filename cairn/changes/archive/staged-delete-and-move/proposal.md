---
cairn: change
id: staged-delete-and-move
status: landed
created: 2026-08-02
---

# Client-staged Remove and Move take effect in the hub

## Why

Driving the pimdir store (io-pimdir over `ReplicaHub`) with himalaya's write
commands surfaced that a **client-staged `Remove` or `Move` is a silent no-op**:
the CLI reports success, but the store records nothing and the next sync carries
nothing. Flags, `Add`, `Copy` and `Edit` all work; only the two
deletion-shaped mutations are lost.

The cause is a broken invariant between the two halves of the hub. Everywhere in
the engine, **a `Tombstone`-status placement means `item.deleted`** — the
projection only ever mints a `Tombstone` from a deleted item
(`tombstone_placement`), and the sync only writes one back for an item still
pending removal. But `absorb_upsert` ignores `placement.status` and
unconditionally runs `item.deleted = false` ("a live upsert resurrects a
delete"). `ReplicaMutation::Remove` and `Move` both stage exactly such a
`Tombstone`-status `UpsertPlacement` — so absorbing it immediately resurrects
the item the mutation meant to delete. The tombstone never reaches storage, so
the projection never asks the sync to push the removal.

`Move` has a second gap. It stages only a source tombstone, trusting the target
collection to "pick it up on its next enumerate" via a preserved `origin`. But a
hub binding stores only `handle` + `base`; `origin` is dropped on absorb, and
cross-collection membership is never auto-propagated (propagation is
within-collection, across sources). So even with the tombstone fixed, a moved
item would delete from the source and appear in the target *nowhere*.

## What

Restore the invariant and make both mutations land, with the smallest change
that fits the existing plumbing:

1. **`absorb_upsert` honors a `Tombstone`-status upsert** as a staged local
   delete: mark the item `deleted`, keep (refresh) the source's binding — its
   `handle` and `base`, so the projection still knows the remote handle to push
   the remove against — and do not resurrect or adopt the tombstone's content.
   A live-status upsert keeps its current resurrect-and-adopt behaviour. This is
   what makes `Remove` (and a `Move`'s source side) actually delete.

2. **`Move` stages a target placement too**, mirroring `Copy`: a `Created`
   placement in the target collection under a caller-supplied `placeholder`,
   carrying the source `origin`, alongside the source tombstone. A move is thus
   a server-side-style copy into the target plus a remove from the source — two
   pushes the next sync derives — rather than one atomic UID MOVE (which the hub
   binding cannot currently carry). The brief cross-side window is harmless and
   copy-before-remove loses nothing.

`Move` gains a `placeholder: ReplicaHandle` field (as `Copy` has), so the caller
names the provisional target handle; himalaya's `move_messages` supplies it.

Not in scope: a truly atomic UID MOVE (needs `origin` plumbed through the
binding, the projection and the remote seam) and the server-side-copy
optimisation for `Copy`/`Move` (both already re-upload the body today, since
`origin` does not survive absorb) — separate, larger changes.
