---
cairn: delta
change: a-no-op-edit-stages-nothing
---

## MODIFIED Requirements
### Requirement: The offline mutation vocabulary
`ReplicaMutation` SHALL stage a local edit to one collection offline, reconciled
on the next sync:

- `SetFlags` — replace a placement's flags and mark it dirty (a pending create
  stays `Created`, an unresolved conflict stays `Conflict`; the flag change rides
  along).
- `Remove` — tombstone a placement, kept until synced. Absorbed as a staged
  delete (the item is marked deleted, its binding kept), so the next sync pushes
  the remove.
- `Edit` — store a new body and repoint the placement at it (full level),
  keeping the base so the next sync derives the push. An edit whose object is
  the one the base already holds stages nothing and SHALL leave the status
  where it found it, `ReplicaPlacement::staged_edit` being the single reading
  of "there is a local content edit here"; every other edit marks the placement
  dirty. Editing a conflicted placement resolves it whatever body it carries,
  the base adopting the remote revision observed at conflict time.
- `Copy` — stage a `Created` placement in a target under a caller-supplied
  `placeholder`, carrying the source origin; the source is untouched.
- `Move` — stage a `Created` placement in the target under a caller-supplied
  `placeholder` (carrying the source origin), **and** tombstone the source. A
  move is thus a copy into the target plus a remove from the source, both derived
  on the next sync; the source's tombstone and the target's create land in their
  respective collection hubs.
- `Add` — see below.

A mutation SHALL touch the local replica only; the remote is reconciled by sync.
