---
cairn: change
id: hub-conflict-round-trip
status: landed
created: 2026-08-07
---

# The hub loses a per-source content conflict

## Why

A placement the merge left conflicted does not survive a round trip through the
hub. `absorb` discards the state and `project` never reproduces it, so a
storage built on `ReplicaHub` (io-pimdir, and therefore neverest) reads a
conflicted placement back as `Dirty`.

Concretely, in `hub.rs`:

- `absorb_upsert` builds every binding as `{ handle, base }`, ignoring the
  incoming placement's `status` and `conflict_revision`. A
  `ReplicaStatus::Conflict` upsert is absorbed as an ordinary live upsert.
- `bound_placement` derives its status purely from `base == item content`, so
  it only ever yields `Clean` or `Dirty`, and hardcodes
  `conflict_revision: None`.

The merge depends on both surviving:

- `sync.rs` leaves an unresolved conflict alone (`if local.status ==
  ReplicaStatus::Conflict` → keep the local edit, do not re-derive a push).
  Read back as `Dirty`, the same push is derived again, rejected again, and
  marked conflicted again — **every run, forever**, with no convergence.
- `remote_state` prefers `conflict_revision` over the base revision when
  synthesizing a remote item, precisely "so it does not regress its conflict
  tracking". Lost, the tracking regresses to the base on every run.
- `pull_flags` and `rebase` both take care to preserve `Conflict` across a
  flag-axis convergence. That care is wasted if the status cannot be stored.

The bug is invisible to an immutable-content backend — mail bodies never
conflict — so nothing exercised it until a CardDAV/CalDAV consumer appeared.
It was found by neverest's phase-3 fake-remote test (change `generic-pim-sync`),
before any DAV code existed.

## What (design)

A per-source content conflict is a property of **one source's binding**, not of
the shared item: it says "this source and *its own remote* diverged", which is
a different fact from `ReplicaHubItem::conflicted`, the *cross-source* conflict
`reconcile_content` owns (two sources edited the shared body differently). The
two must not be conflated — a store with two sources needs both, independently.

So `ReplicaSourceBinding` gains the two fields that mirror the placement:

```rust
pub struct ReplicaSourceBinding {
    pub handle: ReplicaHandle,
    pub base: Option<ReplicaBase>,
    /// This source's own sync left the placement conflicted.
    pub conflicted: bool,
    /// The remote revision observed when it was marked conflicted.
    pub conflict_revision: Option<String>,
}
```

- `absorb_upsert` records `conflicted: placement.status ==
  ReplicaStatus::Conflict` and copies `conflict_revision`, on both the
  tombstone and the live path (a tombstone is never conflicted, so it stores
  `false` naturally, by the same expression).
- `bound_placement` projects `Conflict` when the binding is conflicted, ahead
  of the `Clean`/`Dirty` decision, and carries `conflict_revision` back.
- Resolution needs no special case: the consumer's edit arrives as a `Dirty`
  upsert, which stores `conflicted: false` and clears the revision.

The change is **purely additive** to the cross-source path: `reconcile_content`
and `ReplicaHubItem` are untouched, so existing conflict-policy behaviour is
unchanged.

## Scope / non-goals

- **Persistence is a separate change in io-pimdir.** The hub is the in-memory
  model; io-pimdir maps `ReplicaSourceBinding` onto the `bindings` table, which
  has no column for either field. Until that lands (a pimdir schema change,
  normative in the `pimdir` repo), a pimdir-backed store still loses the
  conflict across a process restart, even though the hub no longer does.
- No change to the cross-source conflict policy (`ReplicaHubConflict`) or to
  `reconcile_content`.
- No change to the merge itself: `sync.rs` already does the right thing given a
  faithful round trip.

## Compatibility

Adding public fields to `ReplicaSourceBinding` breaks its struct literals — the
crate is `0.2.x`, pre-1.0, so this is a minor bump, and io-pimdir is the only
known constructor.
