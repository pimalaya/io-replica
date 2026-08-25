---
cairn: change
change: dedup-rebases
---

# Delta

## ADDED Requirements

### Requirement: A linked body carries the base with it
A `Full` upgrade that resolves a placement's body from the object store instead
of fetching it SHALL record that body as the placement's base content, exactly
as the fetch path does. A placement holding a body its base does not is the
shape of a staged local edit, so a storage projects it dirty and the consumer
re-derives a change nobody made, on every sync, without ever converging.

### Requirement: A mutable body is fetched, never linked
A placement whose base carries a revision SHALL be fetched rather than linked
from the object store: its link id is left out of the lookup, and a hit on it is
ignored. A link id says two copies are the same item, not that they hold the
same bytes, and a source that rewrites bodies in place gives each copy its own
revision, so linking one copy's body under another's would record content no
fetch confirmed. Immutable content keeps the dedup, which is what it is for, and
the object store deduplicates the bytes of a fetched body regardless.

#### Scenario: The same message in two collections downloads once and reads clean
- GIVEN a based placement with no revision whose link id the object store already holds a body for
- WHEN it is upgraded to `Full`
- THEN it is written at `Full` with that body, its base holds the same body, and no fetch is requested

#### Scenario: A revision-carrying placement is fetched
- GIVEN a based placement carrying a revision whose link id the object store already holds a body for
- WHEN it is upgraded to `Full`
- THEN the body is fetched from the remote rather than linked from the store

## MODIFIED Requirements

## REMOVED Requirements
