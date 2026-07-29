---
cairn: delta
change: multi-source-hub
---

## ADDED Requirements

### Requirement: The hub composes single-remote sync into multi-source
The crate SHALL provide a `hub` module — a `ReplicaHub` of logical items keyed by
link id, each carrying shared content (flags, object, meta, level) and a base per
source (`ReplicaSourceBinding`) — with two pure, I/O-free functions:
`project(collection, source)` returns the placements a source's `load` should
return, and `absorb(source, writes)` folds a source's sync writes back. A
projected placement carries the shared content against the source's own base, so
a change another source folded into the hub reads as locally dirty for this
source and the ordinary per-source merge pushes it, with no cross-merge and no
merge-core change. The module is shipped unconditionally (it pulls no
dependencies, so a feature gate is not justified).

### Requirement: Membership propagation is hydration-safe
`project` SHALL stage a `Created` append for an item a source lacks only when the
hub already holds the body, and SHALL never raise a placement's level. A
two-source sync of items already in agreement therefore projects them `Clean` at
their current level with no body, so the engine derives no push and no upgrade
and fetches nothing.

#### Scenario: In-agreement items fetch no bodies
- GIVEN two sources holding a link at the same flags, `Meta` level, no body
- WHEN each source's placements are projected
- THEN both project `Clean` at `Meta` with no object

#### Scenario: A flag change propagates through the hub
- GIVEN two sources bound to one item, agreeing on their base
- WHEN one source's flag change is absorbed
- THEN the other source projects the item dirty, so its next sync pushes the change
