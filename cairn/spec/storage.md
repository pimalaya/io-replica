---
cairn: spec
capability: storage
status: current
---

# Storage

The storage seam is the local index plus blob store the engine reconciles
against. The consumer implements it (a sqlite index plus a blob dir in the
reference driver); the engine reads it through `load` and `lookup_objects` and
mutates it through a `write` batch of [`ReplicaWriteOp`]s. The engine performs no
I/O of its own: storage effects travel as coroutine yields.

### Requirement: DropPlacement is the retention decision point
The engine SHALL signal every removal — a local delete confirmed by the remote,
or a member vanished upstream — as a `ReplicaWriteOp::DropPlacement`, and SHALL
make no other assumption about what the storage does with it. A storage MAY hard-
delete the row, or soft-delete it (retain the row, marked removed-upstream) for a
backup that must never lose a copy the source expunged. No merge-core option
governs this; retention is the storage's choice.

### Requirement: Hiding rows from load is safe
The merge reconciles only the placements a `load` returns. A storage that hides
soft-deleted rows from `load` SHALL therefore not cause the engine to re-derive
against them: the hidden row is invisible on every later sync, delta or full, so
it is neither re-added nor looped over, and the retained copy survives.

#### Scenario: A backup keeps a remote expunge
- GIVEN a storage that soft-deletes on `DropPlacement` and hides such rows from `load`
- AND a member the remote expunges after a first sync
- WHEN the collection is synced again, whether delta or full
- THEN the row is retained but absent from `load`, and no re-derivation occurs
