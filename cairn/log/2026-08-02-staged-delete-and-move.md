---
cairn: log
change: staged-delete-and-move
date: 2026-08-02
---

# Client-staged Remove and Move take effect in the hub

Driving the pimdir store (io-pimdir over `ReplicaHub`) with himalaya's write
commands showed that a client-staged `Remove` or `Move` was a **silent no-op**:
the CLI reported success, the store recorded nothing, and the next sync carried
nothing. Flags, `Add`, `Copy` and `Edit` worked.

## Root cause

The invariant "a `Tombstone`-status placement means `item.deleted`" was broken.
`hub.rs::absorb_upsert` ignored `placement.status` and unconditionally ran
`item.deleted = false` (resurrect), while `mutate.rs` stages both `Remove` and
`Move` as a `Tombstone`-status `UpsertPlacement`. So absorbing the mutation
immediately resurrected the item it meant to delete. `Move` had a second gap: it
staged only a source tombstone, trusting a preserved `origin` for the target —
but the hub binding stores only `handle` + `base`, and cross-collection
membership is never auto-propagated, so the target gained nothing.

## What landed

- **`absorb_upsert` honors a `Tombstone`-status upsert** (capability `hub`): it
  marks the item deleted and keeps the binding (handle + base) so the projection
  pushes the remove, without adopting content or resurrecting. A live-status
  upsert is unchanged. This makes `Remove` (and a `Move`'s source side) delete.
- **`Move` stages a target placement too** (capability `mutate`): a `Created`
  placement in the target under a caller-supplied `placeholder` (origin = source)
  plus the source tombstone — a copy into the target and a remove from the
  source, both derived on the next sync. `ReplicaMutation::Move` gained a
  `placeholder` field (mirroring `Copy`); himalaya's `move_messages` supplies it.

## Verification

- io-replica: new `hub` test (a `Tombstone` upsert marks deleted, keeps the
  binding, projects a `Tombstone`) and rewritten `mutate` test (Move stages
  target `Created` + source `Tombstone`); all 131 tests + property/integration
  suites green.
- io-pimdir: two end-to-end round-trip tests driving `ReplicaMutate` against the
  store — a staged `Remove` empties the read surface yet reopens as a projected
  `Tombstone`; a staged `Move` empties the source, fills the target under the
  same message-scoped `seq`, and projects the two pending pushes. 13 tests green.
- Live (himalaya `--account local` over a real Fastmail-synced pimdir store): a
  `move` of a synced message now leaves the source and appears in the target,
  keeping its public id.

## Not in scope

A truly atomic UID MOVE (needs `origin` plumbed through the binding, projection
and remote seam) and the server-side-copy optimisation for `Copy`/`Move` (both
re-upload the body today, since `origin` does not survive absorb) — separate,
larger changes.

Capabilities moved: `hub`, `mutate`.
