---
cairn: change
id: delete-disposition
status: landed
created: 2026-08-25
---

# What becomes of a delete the source will not take is a decision, not a side effect

## Why

`engine-algorithm-audit` found the one axis where the push switches are not orthogonal. A source can refuse a local delete two ways, and they meant opposite things:

- `push = false` **reverted** it: the replica mirrors a source it does not own, so the member comes back.
- `rights.remove = false` **held** it: the tombstone stayed pending for a later run.

So `ReplicaPushRights::none()` was not `push = false`, and nothing said so. Neither behaviour is wrong; which one a consumer wants depends on why the source refuses, and that is not something to read off two unrelated switches.

## What

- `ReplicaDeletePolicy { Revert, Keep }` on `ReplicaSyncOptions`, consulted wherever a delete cannot go, whether because `push` is false or because `rights` forbids the remove.
- `Revert` is the default, because a held tombstone hides a member the source still holds, for good: an incremental enumeration never lists an untouched member again. A consumer whose refusal is a policy that may lift (an archive taking appends but no deletes today) sets `Keep`.

## Scope / non-goals

- **Behaviour change**: `rights.remove = false` now reverts by default where it held. `Keep` restores it.
- The third disposition the audit floated, retaining a hidden row, stays where it is: soft deletion is the storage's, not the engine's.
