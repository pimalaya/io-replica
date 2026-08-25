---
cairn: spec
capability: coroutine
status: current
---

# Coroutine

Every verb in the crate is a state machine implementing one contract: a driver resumes it, it yields what it wants (`ReplicaYield`), the driver services that and resumes it with the matching reply (`ReplicaArg`), until it completes. The engine performs no I/O of its own, which is what the contract is for: storage and remote are `Wants` variants rather than traits injected into the engine.

The rules here are about the contract itself, not about what any one verb decides. What the verbs decide lives under [sync](sync.md), [upgrade](upgrade.md), [rekey](rekey.md), [mutate](mutate.md), [hub](hub.md) and [storage](storage.md).

The five verbs are `ReplicaOpen`, `ReplicaUpgrade`, `ReplicaMutate`, `ReplicaSync` and `ReplicaRekey`. `ReplicaOpen` is the one with no capability file of its own, because it decides nothing: it is the offline read, a `WantsLoad` of a whole collection answered straight back to the caller, and it exists so a consumer projecting a replica needs no second code path for "read it without touching the network". Its rules are the contract's own.

### Requirement: An offline read is a verb
Reading a collection without a remote SHALL go through the same coroutine contract as everything else (`ReplicaOpen`), rather than through a direct storage call. A consumer holding the engine holds one way to reach a replica, so a projection built offline and one built during a sync cannot drift apart, and a storage implements one seam rather than a seam plus a back door.

`ReplicaOpen` SHALL scope its load to the whole collection: it is one of the two verbs that reason about what is *missing* from a replica rather than about named rows, its answer being the projection itself.

### Requirement: One error for a broken coroutine contract
A driver that resumes a coroutine with an arg not matching the pending yield, or without the arg the yield required, SHALL be told so through `ReplicaArgError`. It is one type for every verb because it is one bug, in the driver rather than in the coroutine, and the caller knows which verb it resumed.

`ReplicaOpen`, `ReplicaUpgrade`, `ReplicaRekey` and `ReplicaSync` SHALL return it directly: they read local state, ask for remote state and stage writes, and none of that can fail inside the engine. A verb with failures of its own (`ReplicaMutate`) SHALL compose it beside them rather than restate its variants.

### Requirement: A completed coroutine does not resume
Every coroutine SHALL hold a terminal state and answer a resume after completion with `ReplicaArgError::UnexpectedArg`. Handing back a default output instead is worse than useless: an empty report or an `Ok(())` is exactly what a run that genuinely did nothing returns, so a driver with a loop bug is told it succeeded.

#### Scenario: A driver resumes a finished run
- GIVEN a coroutine that completed
- WHEN the driver resumes it again
- THEN it answers `UnexpectedArg` rather than an empty success
