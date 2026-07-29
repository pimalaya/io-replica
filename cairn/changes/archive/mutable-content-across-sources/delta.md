---
cairn: delta
change: mutable-content-across-sources
---

## ADDED Requirements

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
