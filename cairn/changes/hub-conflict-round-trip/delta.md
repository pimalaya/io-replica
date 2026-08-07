---
cairn: change
change: hub-conflict-round-trip
---

# Delta

## ADDED Requirements

### Requirement: A per-source content conflict round-trips through the hub
A placement absorbed with `ReplicaStatus::Conflict` SHALL be projected back as
`ReplicaStatus::Conflict`, carrying the `conflict_revision` it was absorbed
with. The state SHALL be held on the source's binding, not on the shared item:
it records that *this source* and its own remote diverged, which is a distinct
fact from the item-level cross-source conflict.

A conflicted binding SHALL take precedence over the base comparison when the
status is derived, so a conflict is never downgraded to `Clean` or `Dirty`. An
upsert of any other status SHALL clear it, so a consumer resolving the conflict
with an edit needs no explicit resolution call.

This is what lets the merge honour its own rule that an unresolved conflict is
left alone: without it, the same push is re-derived, re-rejected and re-marked
on every run, and a consumer reading the storage cannot tell which items need
resolving.

## MODIFIED Requirements

None.

## REMOVED Requirements

None.
