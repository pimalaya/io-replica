---
cairn: delta
change: a-bound-create-is-still-a-create
---

## ADDED Requirements
### Requirement: A binding with no base is a pending create
`ReplicaHub::project` SHALL read a bound item whose binding holds no base as `ReplicaStatus::Created`, on the same condition `created_placement` applies to an unbound one: the hub holds the body. A binding's base is what its source last reconciled with its own remote, so a binding without one has never reached that source and the item is still the create it was staged as.

Without it the hub cannot represent a persisted create at all. `absorb` binds every live upsert whatever its status, and the merge derives an add for a `Created` placement alone, so a create written through a hub-backed store is neither pushed nor dropped on any run after the first: a locally-authored item never leaves the machine, and the `Created` placement an edit-beats-delete resurrection writes is refused by the next pass, which loses the edit.

#### Scenario: A locally-authored create reaches its own source
- GIVEN a source over a hub-backed store staging an `Add`
- WHEN the store is written and the source's placements projected again
- THEN the placement reads `Created`, and the next sync appends it to that source's own remote
