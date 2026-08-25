---
cairn: delta
change: engine-compaction
---

## ADDED Requirements

### Requirement: One error for a broken coroutine contract
A driver that resumes a coroutine with an arg not matching the pending yield, or without the arg the yield required, SHALL be told so through `ReplicaArgError`. It is one type for every verb because it is one bug, in the driver rather than in the coroutine, and the caller knows which verb it resumed.

`ReplicaOpen`, `ReplicaUpgrade`, `ReplicaRekey` and `ReplicaSync` SHALL return it directly: they read local state, ask for remote state and stage writes, and none of that can fail inside the engine. A verb with failures of its own (`ReplicaMutate`) SHALL compose it beside them rather than restate its variants.

### Requirement: A completed coroutine does not resume
Every coroutine SHALL hold a terminal state and answer a resume after completion with `ReplicaArgError::UnexpectedArg`. Handing back a default output instead is worse than useless: an empty report or an `Ok(())` is exactly what a run that genuinely did nothing returns, so a driver with a loop bug is told it succeeded.

#### Scenario: A driver resumes a finished run
- GIVEN a coroutine that completed
- WHEN the driver resumes it again
- THEN it answers `UnexpectedArg` rather than an empty success

### Requirement: A hub projection states only what its source decides
The three placements the hub projects (a bound member, a tombstone for one deleted elsewhere, a create for one this source lacks) SHALL be built from one projection carrying the item's shared content, each settling only what the source's binding decides: the status it reads as, its base, its conflict revision, the handles it cannot resolve.

Stating the shared content once is what makes a field added to `ReplicaPlacement` a change in one place: three hand-written projections make forgetting one a silent wrong answer rather than a compile error.
