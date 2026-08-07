---
cairn: spec
capability: hub
status: current
---

# Hub

The `hub` module composes the single-remote merge into multi-source sync (mirror,
two-way) without a bespoke cross-merge. A `ReplicaHub` holds logical items keyed
by link id; each item carries shared content (flags, object, meta, level) and a
base per source. A storage wraps the hub, projecting a per-source view on `load`
and absorbing the engine's writes back on `write`, so propagation falls out of
the ordinary per-source reconcile. The full design and the deferred parts
(cross-source delete propagation, mutable content across sources) are in
`docs/MULTISOURCE.md`.

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

### Requirement: The hub resolves cross-source content conflicts by policy
`ReplicaHubItem` SHALL carry a `conflicted` flag and a `conflict_object`, and
`ReplicaHub` a `ReplicaHubConflict` policy (`Manual`, `PreferIncoming`,
`PreferExisting`; default `Manual`). On an upsert, the hub SHALL compare the
incoming body against the source's last-synced shared body and the hub's current
shared body: when both moved to different bodies since the source last agreed,
that is a conflict. `Manual` SHALL flag it and record the diverging body,
preserving both and keeping the shared body; `PreferIncoming` SHALL adopt the
incoming body; `PreferExisting` SHALL keep the shared body. A clean fast-forward
(only the source changed) SHALL adopt the incoming body. Flags are unaffected
(element-wise, never conflicting), and immutable-content backends mint a new link
id per body and never reach this path.

#### Scenario: A divergent edit conflicts and preserves both under Manual
- GIVEN two sources agreeing on a body, then each editing it to a different body
- WHEN both upserts are absorbed under `Manual`
- THEN the item is `conflicted`, the shared body is kept, and the diverging body is recorded

#### Scenario: A clean fast-forward adopts the new body
- GIVEN two sources agreeing on a body, then only one editing it
- WHEN that upsert is absorbed
- THEN the hub adopts the new body with no conflict

### Requirement: A per-source content conflict round-trips through the hub
`ReplicaSourceBinding` SHALL carry a `conflicted` flag and a
`conflict_revision`, recording that **this source and its own remote** diverged
and the merge left the placement `Conflict`. This is a distinct fact from the
item-level cross-source conflict above — one says "left and its server
disagree", the other "left and right disagree" — and a two-source store needs
both independently; neither SHALL set the other.

`absorb` SHALL record both from any upsert whose status is `Conflict`, and clear
them for an upsert of any other status, so a consumer resolving the conflict
with an ordinary edit needs no dedicated resolution call. `project` SHALL yield
`Conflict` for a conflicted binding **ahead of** the base comparison, carrying
the stored `conflict_revision` back, so a conflict is never downgraded to
`Clean` or `Dirty`.

Without this the merge cannot honour its own rule that an unresolved conflict is
left alone: the placement reads back `Dirty`, the engine re-derives the push the
remote already rejected, and the same conflict is re-marked on every run with no
convergence — while a consumer reading the storage cannot tell which items need
resolving. Immutable-content backends never reach this path.

#### Scenario: A conflicted placement keeps its status and revision
- GIVEN a source whose merge marked a placement `Conflict` at an observed remote revision
- WHEN that upsert is absorbed and the source's placements are projected
- THEN the placement projects `Conflict` carrying the same `conflict_revision`

#### Scenario: A conflict outranks the base comparison
- GIVEN a conflicted binding whose base still equals the shared content
- WHEN the source's placements are projected
- THEN the placement projects `Conflict`, not `Clean`

#### Scenario: An edit resolves the conflict
- GIVEN a conflicted binding
- WHEN an upsert of any other status is absorbed for that source
- THEN the binding is no longer conflicted and carries no `conflict_revision`
