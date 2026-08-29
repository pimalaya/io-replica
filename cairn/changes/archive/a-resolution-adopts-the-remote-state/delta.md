---
cairn: delta
change: a-resolution-adopts-the-remote-state
---

## MODIFIED Requirements
### Requirement: The offline mutation vocabulary
(mutate) The `Edit` bullet states that editing a conflicted placement resolves
it whatever body it carries, and that the base adopts the whole remote state the
resolution was merged against: the revision observed at conflict time and the
body recorded beside it.

### Requirement: A conflict keeps the body it diverged from
(sync) The recorded pair is taken into the base together on resolution rather
than dropped, the base being what the sync measures the resolution against.

### Requirement: The hub resolves cross-source content conflicts by policy
(hub) A source leaving a conflicted binding SHALL count as having changed its
body whatever that body is, the "a source cannot diverge from itself" rule being
read from the status rather than from the body alone.

## ADDED Requirements
### Requirement: A resolution is measured against the remote it settled
(mutate) The base a resolution leaves SHALL be the remote state it was merged
against, both halves of it, and a conflict holding no base SHALL be given one
from the same pair.
