---
cairn: change
id: a-no-op-edit-stages-nothing
status: landed
created: 2026-08-29
---

# An edit restating the synced body stages nothing

## Why

`ReplicaMutation::Edit` marks its placement `ReplicaStatus::Dirty` without ever comparing the incoming body against the one the base already holds. An edit that re-asserts the synced content therefore mints a placement that is `Dirty` while `ReplicaPlacement::staged_edit` reads `None`, and that method documents itself as "the single reading of 'there is a local content edit here', so every caller asks the same question". Every other site in the crate asks it the same way and disagrees with `mutate`: `sync` derives a content push from `local.object != base.object` (src/sync.rs:664), the remote-delete resurrection from the same comparison (src/sync.rs:528), `rekey` likewise (src/rekey.rs:138, src/rekey.rs:201), and the flag merge already exists only to scrub the spurious status back to `Clean` on the next sync (src/sync.rs:818).

Two consequences are visible. A consumer rendering status shows unsynced changes for a write that changed nothing, until some later sync cleans it. And the sync's edit-beats-delete rule, which consults `staged_edit`, reads the placement as flags-only and drops it when the remote deletes the item, while the status claims a pending push. No bytes are lost in that shape: the content the placement claims is byte-identical to the one the remote held before deleting it, so there is nothing to resurrect. The defect is the internal disagreement, not a loss.

The mutable-content property model fails on it, roughly once in 100000 generated cases today and once in 10000 under the generator reweighting queued behind this change (see the campaign findings). The model is wrong too, on the same fact from the other side: it records an edit intent whenever `mutate` returns `Ok`, so it demands that the server carry a body the engine never staged.

## What

Guard the dirtying in `ReplicaMutate::writes` on the object having actually changed: an `Edit` whose object is the one the base already holds leaves the status where it found it. Resolving a conflict stays dirty either way, the divergence it settles being the change, so the conflict branch keeps its current outcome exactly.

Correct the model beside it: `MutOp::LocalEdit` and the resolution loop record an edit intent only when the placement holds a staged edit afterwards, which is the reading the ledger's own `void_superseded_edits` predicate already uses.

Pin the delta-debugged five-op sequence as a regression test.
