---
cairn: log
change: a-bound-create-is-still-a-create
landed: 2026-08-29
---

# A create the hub bound reads back as a create

`ReplicaHub::project` produced `ReplicaStatus::Created` in one place, the arm for an item the source holds no binding for. `absorb` binds every live upsert whatever its status, so the first write of a pending create took a binding and every read after it answered `Dirty`, and the merge derives an add for a `Created` placement alone. The create was then neither pushed nor dropped, on that run and on every run after it, with nothing reported and a successful exit.

`bound_placement` now reads a binding with no base as the pending create it is, guarded on the hub holding the body exactly as the unbound projection is. The base is what a source last reconciled with its own remote, and every production write of a base-less live placement is a create: `mutate`'s `Add`, `Copy` and `Move`, the sync's and the rekey's resurrections, and the keep-both fork. A pulled row carries a base from the moment it is written, and a rekey carries one over, so no other state is caught by the reading.

Three user-visible losses close with it: a locally-authored item never leaving the machine it was composed on, an edit-beats-delete resurrection written by a pull pass and refused by the push pass that follows, and a keep-both fork staged and never delivered. The first two were seen against a real CardDAV server by the Fastmail end-to-end track.

The new hub property test finds it unaided at the default case count, shrinking to a single locally-authored create; tests/hub.rs pins the round trip by hand beside it.

Spec updated: `hub` (ADDED: a binding with no base is a pending create).
