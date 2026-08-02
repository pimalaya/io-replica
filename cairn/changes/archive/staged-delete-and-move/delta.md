---
cairn: change
change: staged-delete-and-move
---

## MODIFIED Requirements

### Requirement: The hub propagates a delete across sources
`ReplicaHubItem` SHALL carry a `deleted` flag. An item becomes deleted two ways,
both feeding the same projection: when `absorb` sees a `DropPlacement` of a bound
member (a member removed under the source's feet), it SHALL mark the item deleted
and remove that source's binding; and when `absorb` sees an `UpsertPlacement`
whose `status` is `Tombstone` (a client-staged `Remove`, or a `Move`'s source
side), it SHALL mark the item deleted and **keep** the source's binding — its
`handle` and `base` — so the projection knows the remote handle to push the
remove against, without adopting the tombstone's content or clearing the delete.
`project` SHALL then yield a `Tombstone` placement (keeping the content, so
edit-beats-delete still applies) for every source that still holds the item, and
nothing for a source that lacks it (a deleted item is never copied). Once no
source holds the item it is pruned. A **live-status** upsert SHALL clear
`deleted`, so a re-add or an edit-beats-delete resurrection brings the item back
on every source.

#### Scenario: A delete propagates as a tombstone
- GIVEN two sources holding one item, and one source removing it
- WHEN the other source's placements are projected
- THEN it projects a `Tombstone` for that item, so its next sync pushes a remove

#### Scenario: A client-staged remove marks the item deleted
- GIVEN an item bound to a source
- WHEN an `UpsertPlacement` with `Tombstone` status is absorbed for that source
- THEN the item is deleted, its binding is kept, and the source projects a `Tombstone`

#### Scenario: A live upsert resurrects a delete in flight
- GIVEN an item marked deleted on one source
- WHEN a live upsert for it is absorbed from another source
- THEN the item is no longer deleted and is copied back to the sources that lack it

### Requirement: The offline mutation vocabulary
`ReplicaMutation` SHALL stage a local edit to one collection offline, reconciled
on the next sync:

- `SetFlags` — replace a placement's flags and mark it dirty (a pending create
  stays `Created`, an unresolved conflict stays `Conflict`; the flag change rides
  along).
- `Remove` — tombstone a placement, kept until synced. Absorbed as a staged
  delete (the item is marked deleted, its binding kept), so the next sync pushes
  the remove.
- `Edit` — store a new body and repoint the placement at it (full level, dirty),
  keeping the base so the next sync derives the push; editing a conflicted
  placement resolves it, the base adopting the remote revision observed at
  conflict time.
- `Copy` — stage a `Created` placement in a target under a caller-supplied
  `placeholder`, carrying the source origin; the source is untouched.
- `Move` — stage a `Created` placement in the target under a caller-supplied
  `placeholder` (carrying the source origin), **and** tombstone the source. A
  move is thus a copy into the target plus a remove from the source, both derived
  on the next sync; the source's tombstone and the target's create land in their
  respective collection hubs.
- `Add` — see below.

A mutation SHALL touch the local replica only; the remote is reconciled by sync.
