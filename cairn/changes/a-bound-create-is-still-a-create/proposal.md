---
cairn: change
id: a-bound-create-is-still-a-create
status: landed
created: 2026-08-29
---

# A create the hub bound reads back as a create

## Why

`ReplicaHub::project` dispatches on whether the source holds a binding. `created_placement`, the only place `ReplicaStatus::Created` is ever produced, runs on the arm where the binding is absent; a source that holds one gets `bound_placement`, which answers `Conflict`, `Clean` or `Dirty` and nothing else. `absorb_upsert` binds every live upsert whatever its status, so the first write of a pending create takes a binding, and every read after it says `Dirty`.

The merge derives an add for a `Created` placement alone (the `(true, false, false)` arm of `Sync::merge`), so a `Dirty` one is neither pushed nor dropped: it is reconsidered identically on every run, for good. Nothing reports it, the run exits successfully, and the consumer keeps reporting the same pending change.

Three shapes reach it, and all three are data the user asked for:

- a locally-authored create (`ReplicaMutation::Add`, a compose, an import, a queued action) never leaves the machine it was authored on;
- the edit-beats-delete resurrection writes a `Created` placement and, when the pass that wrote it was not allowed to push, the pass that is reads it back `Dirty` and refuses it, which loses the edit that beat the delete;
- a keep-both fork (`stage_conflict_dup`) is staged and never delivered.

Confirmed against a real CardDAV server by the Fastmail end-to-end track: six queued creates reported as applied and never sent, and a resurrecting edit reported as pushed while the server answered 404. Confirmed here by the new hub property test, which fails at the default case count and shrinks to a single op, a locally-authored create that reaches every source except the one it was authored on.

## What

`bound_placement` reads a binding with no base as the pending create it is: that binding has never been reconciled with the source's own remote, so the item is not there yet. `ReplicaSourceBinding::base` is documented as exactly that, "the last state synced with this source, `None` until first reconciled", and every production write of a base-less live placement is a create: `mutate`'s `Add`, `Copy` and `Move`, the sync's and the rekey's resurrections, and the keep-both fork. Everything a sync pulls carries a base from the moment it is written (`pull_add`), and `carry` gives one to every row a rekey keeps.

The projection keeps the guard `created_placement` already applies, a body the hub actually holds, so an item whose bytes are missing is not offered as an add nobody can push.

## Alternatives weighed

Recording the status on the binding was the obvious other answer and is worse: it stores a derived fact beside the two bases it is derived from, and a consumer persisting bindings would have to migrate for it. The base already answers the question.

Reading "no base" as a create is exact rather than conservative. The one state it could have misread, a binding taken between an enumerate and its reconcile, does not exist: an enumerated row is written with its base, and a row with no link id is not hubbed at all.
