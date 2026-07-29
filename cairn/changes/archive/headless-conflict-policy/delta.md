---
cairn: delta
change: headless-conflict-policy
---

## ADDED Requirements

### Requirement: Headless conflict resolution
`ReplicaSyncOptions` SHALL carry a `ReplicaConflictPolicy` (`Manual`,
`PreferLocal`, `PreferRemote`, `KeepBoth`; default `Manual`) applied when content
diverges on both sides of a based placement. `Manual` marks the placement
conflicted and waits for the consumer's edit. `PreferRemote` drops the local
edit and pulls the remote. `PreferLocal` pushes the local body as an `Update`
gated on the *observed* remote revision (overwriting the current remote), and
falls back to `Manual` when the source may not push content. `KeepBoth` pulls the
remote into the placement and stages the local body as a fresh `Created` member
so neither version is lost. A base-less create-collision is always kept as a
conflict regardless of the policy. Immutable-content backends report no revision
and so never reach a content conflict.

#### Scenario: PreferRemote discards the local edit
- GIVEN a based placement edited locally and changed on the remote, `conflict = PreferRemote`
- WHEN the collection is synced
- THEN the remote content is pulled and no conflict is recorded

#### Scenario: KeepBoth preserves both versions
- GIVEN a based placement edited locally and changed on the remote, `conflict = KeepBoth`
- WHEN the collection is synced
- THEN the remote is pulled into the placement
- AND the local body is staged as a new `Created` member for the next sync to append
