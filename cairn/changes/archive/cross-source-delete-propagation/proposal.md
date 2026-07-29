---
cairn: change
id: cross-source-delete-propagation
status: landed
created: 2026-07-28
---

# Cross-source delete propagation in the hub

## Why
The first-cut `hub` propagates adds and flags but not deletes: a member the user
(or the server) removes on one source only drops that source's binding, and the
item lives on forever on the other sources. Mirror and two-way sync must delete
it everywhere.

## What
Give `ReplicaHubItem` a `deleted` flag. When `absorb` sees a `DropPlacement` of a
bound member, mark the item `deleted` and remove that source's binding. `project`
then yields a `Tombstone` placement for every source that still holds the item
(so its next sync pushes a `Remove`), and yields nothing for a source that lacks
it (a deleted item is never copied). Once every source has propagated the delete
the item is pruned. A later live upsert clears `deleted` (edit- and add-beats-
delete across sources): if one source re-adds or the engine resurrects it under
edit-beats-delete, it comes back everywhere.
