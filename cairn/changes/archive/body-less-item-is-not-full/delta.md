---
cairn: change
change: body-less-item-is-not-full
---

# Delta

## ADDED Requirements

### Requirement: A body-less item is not `Full`
[`ReplicaLevel::Full`] SHALL mean the item has a stored body. An item holding
none SHALL therefore read at most `Meta`, however far any source got, both where
`absorb` records the level and where `project` reports it.

The level is otherwise the high-water mark across sources, merged as a maximum
so a source that has only probed an item cannot un-know what another one read.
A dropped body is not that kind of absence: `pull_content` drops the stale body
of an item whose remote content changed and lowers the placement so an upgrade
refetches it, and an upgrade skips whatever already reads as `Full`. Left
conflated, the item claims a body it does not have, keeps the summary of the
revision before the change, and no fetch is ever derived for it.

Projecting the same rule is what lets a store already written in that state
heal, since an upgrade reads what `load` projects rather than the stored row.

#### Scenario: A refreshed item stops claiming its lost body
- GIVEN a hub item at `Full` with a stored body, bound to one source
- WHEN that source absorbs the refresh of a remote content change (no body, level lowered)
- THEN the item holds no body and reads `Meta`, and projects `Meta`

#### Scenario: A body-less item stored as `Full` projects below it
- GIVEN a hub item recorded at `Full` whose body is absent
- WHEN its source's placements are projected
- THEN the placement reads `Meta`, so the next upgrade refetches the body

## MODIFIED Requirements

## REMOVED Requirements
