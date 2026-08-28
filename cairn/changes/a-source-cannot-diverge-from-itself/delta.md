---
cairn: delta
change: a-source-cannot-diverge-from-itself
---

## ADDED Requirements

### Requirement: A binding records what it last agreed with the hub on
*Folds into hub.md.*

`ReplicaSourceBinding` SHALL carry `shared_object`, the hub's shared body at this source's last absorbed upsert, and the hub SHALL treat it as the base of the cross-source merge. Every live upsert SHALL move it to the shared body the reconcile settled on, whether that body was adopted, kept, or refused; a `Tombstone` adopts no content and SHALL leave it where it stood. Until a source has folded once it is `None`, and the sync base stands in for it.

This is a second base, for the second axis, and the two cannot be one field. `base` is what the source last agreed with its own remote and only a sync moves it, so it stays behind while an unpushed edit waits, which is what keeps the push derivable. The shared axis needs the opposite: a body this source folded in is agreed with, pushed or not.

#### Scenario: A binding that has never folded
- GIVEN a bound source whose binding carries no `shared_object`
- WHEN an upsert is absorbed for it
- THEN the cross-source comparison is made against its sync base

## MODIFIED Requirements

### Requirement: The hub resolves cross-source content conflicts by policy
`ReplicaHubItem` SHALL carry a `conflicted` flag and a `conflict_object`, and
`ReplicaHub` a `ReplicaHubConflict` policy (`Manual`, `PreferIncoming`,
`PreferExisting`; default `Manual`). On an upsert, the hub SHALL compare the
incoming body against the source's own sync base and the hub's shared body
against what that source last agreed with the hub on: a conflict is the source
having changed its body **and** another source having moved the shared body,
to different bodies. `Manual` SHALL flag it and record the diverging body,
preserving both and keeping the shared body; `PreferIncoming` SHALL adopt the
incoming body; `PreferExisting` SHALL keep the shared body. A clean
fast-forward (only the source changed) SHALL adopt the incoming body, and an
upsert carrying the shared body itself settles nothing either way.

A source SHALL NOT diverge from itself. Its own body folded into the hub and
not yet pushed leaves its sync base behind the shared body, which is the gap
another source folding in also leaves, and reading the two as one drops the
source's next edit: a second offline edit under `Manual`, or the edit that
resolves a conflicted binding, whose merged body would then never be pushed.

Flags are unaffected (element-wise, never conflicting), and immutable-content
backends mint a new link id per body and never reach this path.

#### Scenario: A divergent edit conflicts and preserves both under Manual
- GIVEN two sources agreeing on a body, then each editing it to a different body
- WHEN both upserts are absorbed under `Manual`
- THEN the item is `conflicted`, the shared body is kept, and the diverging body is recorded

#### Scenario: A clean fast-forward adopts the new body
- GIVEN two sources agreeing on a body, then only one editing it
- WHEN that upsert is absorbed
- THEN the hub adopts the new body with no conflict

#### Scenario: A second offline edit is not a divergence
- GIVEN one source whose offline edit the hub adopted, with the push still pending
- WHEN a second offline edit from that source is absorbed
- THEN the hub adopts it, and the item is not conflicted

#### Scenario: A resolving edit becomes the shared body
- GIVEN a conflicted binding whose local body the hub holds as the shared one
- WHEN the merged body is absorbed as an ordinary edit
- THEN it becomes the shared body, so the next run pushes the merge rather than the body it replaced

### Requirement: A per-source content conflict round-trips through the hub
`ReplicaSourceBinding` SHALL carry a `conflicted` flag and a
`conflict_revision`, recording that **this source and its own remote** diverged
and the merge left the placement `Conflict`. This is a distinct fact from the
item-level cross-source conflict above — one says "left and its server
disagree", the other "left and right disagree" — and a two-source store needs
both independently; neither SHALL set the other.

`absorb` SHALL record both from any upsert whose status is `Conflict`, and clear
them for an upsert of any other status, so a consumer resolving the conflict
with an ordinary edit needs no dedicated resolution call. That edit SHALL also
be adopted as the shared body: a binding cleared of its conflict while the item
still holds the body the merge replaced leaves the next run pushing the
unmerged body over the remote the merge was made against. `project` SHALL yield
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
- THEN the binding is no longer conflicted, carries no `conflict_revision`, and its body is the shared one

## REMOVED Requirements

None.
