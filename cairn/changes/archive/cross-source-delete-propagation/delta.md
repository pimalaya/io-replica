---
cairn: delta
change: cross-source-delete-propagation
---

## ADDED Requirements

### Requirement: The hub propagates a delete across sources
`ReplicaHubItem` SHALL carry a `deleted` flag. When `absorb` sees a
`DropPlacement` of a bound member, it SHALL mark the item deleted and remove that
source's binding. `project` SHALL then yield a `Tombstone` placement (keeping the
content, so edit-beats-delete still applies) for every source that still holds
the item, and nothing for a source that lacks it (a deleted item is never
copied). Once no source holds the item it is pruned. A later live upsert SHALL
clear `deleted`, so a re-add or an edit-beats-delete resurrection brings the item
back on every source.

#### Scenario: A delete propagates as a tombstone
- GIVEN two sources holding one item, and one source removing it
- WHEN the other source's placements are projected
- THEN it projects a `Tombstone` for that item, so its next sync pushes a remove

#### Scenario: A live upsert resurrects a delete in flight
- GIVEN an item marked deleted on one source
- WHEN a live upsert for it is absorbed from another source
- THEN the item is no longer deleted and is copied back to the sources that lack it
