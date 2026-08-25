---
cairn: change
change: engine-algorithm-audit
---

# Delta

## ADDED Requirements

### Requirement: A move delivers exactly one copy
A move is staged as a create in the target plus a remove of the source, each derived by its own collection's sync in whichever order the consumer runs them, and both halves can deliver the item on their own: the create by copying from its origin, the remove by relocating the member into its destination. `ReplicaChange::Remove` SHALL therefore carry the `link_id` its destination would receive, and a consumer SHALL relocate only while that destination does not already hold it; otherwise the create already delivered, and the remove is a plain delete of the source.

Neither half may be dropped in favour of the other: the remove is what keeps a move safe when the target syncs last, since the source is relocated rather than deleted out from under a copy that never ran, and the create is what keeps a move working through a hub, whose bindings carry no origin.

An item whose link id is not resolved yet has no such key, so `ReplicaMutation::Move` SHALL stage the source half alone for it: the relocation delivers it, and the target picks it up on its next enumerate.

#### Scenario: The target syncs first
- GIVEN a linked member moved into a target
- WHEN the target's sync copies it, then the source's sync derives its remove
- THEN the destination already holds the link id, so the source is deleted rather than relocated, and the target holds exactly one member

#### Scenario: The source syncs first
- GIVEN the same move
- WHEN the source's sync runs first
- THEN the member is relocated into the target, and the target's create finds its origin gone: it is rejected and stays visibly pending rather than delivering a second copy

#### Scenario: A never-fetched item
- GIVEN a member whose link id is not resolved
- WHEN it is moved
- THEN only the source tombstone is staged, and the target holds exactly one member in either sync order

### Requirement: A read-only source reverts a local delete
Where `ReplicaSyncOptions::push` is false a local delete can never propagate and the replica mirrors the source, so the merge SHALL revert the tombstone rather than apply it. Applying it and waiting for a later enumerate to re-add the member only works against a complete snapshot: an incremental enumerate never lists an untouched member again, so the dropped row would never come back, leaving the replica permanently short of an item the source still holds. Reverting also keeps whatever the placement had cached.

#### Scenario: A delta enumerate
- GIVEN a read-only source with a locally deleted member, unchanged upstream
- WHEN an incremental enumerate lists nothing
- THEN the placement is written back clean, keeping its body

### Requirement: A drop says whether the item is gone
`ReplicaWriteOp::DropPlacement` SHALL carry a `ReplicaDropReason`: `Deleted` when the item itself is gone (a local delete the remote confirmed, a member that vanished upstream), `Superseded` when only this row is gone, replaced by another the same batch writes (a provisional placeholder reconciled to its server-assigned handle, a spine rebuilt onto a new handle space).

A storage that shares one item across sources SHALL propagate a delete only for `Deleted`. Reading a superseded row as a delete turns housekeeping into data loss on every other source, and it is what made the drop-then-upsert order of a rebuilt spine load-bearing.

#### Scenario: A rebuilt spine is not a mass delete
- GIVEN an item bound to two sources
- WHEN one source's row is dropped as `Superseded`
- THEN the item is not marked deleted and the other source projects it unchanged

### Requirement: A load states what it needs
`ReplicaYield::WantsLoad` SHALL carry a `ReplicaLoadScope`: `All`, `Handles`, or `Link`. A mutation asks for the one placement it edits, or, for an `Add`, every row holding the link id it must not collide with; an upgrade asks for the handles it raises; only the merge and the rebuild ask for the whole collection, because only they reason about what is missing from it.

The scope is a floor, not a ceiling: a storage SHALL return at least the placements it names and MAY return more, so a storage that ignores it stays correct. Under-delivering is not correct: a mutation that cannot see a colliding link id creates a duplicate.

#### Scenario: A one-row edit reads one row
- GIVEN a storage that honours the scope exactly
- WHEN any mutation other than `Add` runs
- THEN it is served only the placement it names, and still produces the same writes

### Requirement: A keep-both duplicate is a new item
The duplicate a `KeepBoth` resolution stages SHALL carry an identity derived from the body it forked, both as its provisional handle and as its link id. It is a new item rather than another copy of the one it forked from, since the two hold different bodies, and giving it the original's link id would have a storage sharing items by link collapse the fork back. Deriving both from the body is also what makes two resolutions staged before either is pushed keep both versions, and what gives the retried add an idempotency key.

#### Scenario: Two resolutions of one handle
- GIVEN two keep-both resolutions of the same placement, forking different bodies
- WHEN both are staged before either is pushed
- THEN their handles and link ids differ, so neither overwrites the other

## MODIFIED Requirements

### Requirement: An upgrade revisits what it never got
An upgrade SHALL revisit a placement whose level claims a tier it does not hold: `Full` with no object, `Meta` with no summary. The level is a claim and the payload is the fact, and nothing else revisits what already reads as reached, so such a row would be skipped for good.

#### Scenario: A body-less full row
- GIVEN a placement recorded at `Full` holding no object
- WHEN it is upgraded to `Full`
- THEN it is fetched rather than skipped

### Requirement: Both axes reconcile, every run
The flag axis SHALL run for every placement present on both sides, including one whose content axis derived a push. A push result is matched by handle, so one handle yields at most one change: the flag axis withholds its own push in that case, but still merges and writes. Skipping it outright loses a remote flag change until some later run happens to list the item again, which an incremental enumerate may never do.

#### Scenario: A remote flag change beside a local content edit
- GIVEN a placement with a staged content edit whose remote also changed a flag
- WHEN the sync derives the content push
- THEN the merged flags are written in the same batch

### Requirement: A push is counted when it matched
`ReplicaSyncReport::pushed` SHALL count the changes this run derived and the remote accepted, not the results the consumer reported: a result naming a handle nobody pushed, or naming one twice, cannot inflate it.

## REMOVED Requirements

### Requirement: A collection carries metadata
`ReplicaCollection` is removed. It was referenced nowhere, and its `enumerated` flag stated an invariant the engine models nowhere: spine completeness comes off the consumer's snapshot on every run, never off a stored flag.
