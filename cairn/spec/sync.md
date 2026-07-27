---
cairn: spec
capability: sync
status: current
---

# Sync

`sync` reconciles one collection's local replica against one remote through a
three-way merge of Local, Base and Remote per placement, keyed on the handle.
Flags merge element-wise and never conflict; only divergent mutable content is
kept as a conflict. It is tuned by `OfflineSyncOptions`.

### Requirement: Three-way reconcile
The engine SHALL merge each candidate placement over `(local, base, remote)`,
pushing local-won changes and pulling remote-won changes, comparing per-placement
identities (the flag set and, for mutable-content backends, a content revision)
rather than raw bytes.

### Requirement: Push-outcome discipline
The engine SHALL confirm a push with the remote before rewriting local state: a
flag, content, delete or create push is stashed and applied only on an
`Accepted` outcome; a `Rejected` (or unreported) outcome leaves the placement
dirty, tombstoned or provisional so the next sync retries it.

### Requirement: Push direction
`OfflineSyncOptions.push` SHALL be the master push switch. When false the source
is treated read-only: local flag and content changes are kept dirty and never
pushed, remote-won changes are still pulled, and a local delete is applied to the
replica only. When true, the `OfflinePushRights` refinement SHALL gate each push
kind independently.

#### Scenario: Read-only source keeps local edits
- GIVEN a placement with a locally-changed flag set and `push = false`
- WHEN the collection is synced
- THEN the engine emits no push and the placement stays dirty

### Requirement: Granular push rights
`OfflineSyncOptions` SHALL carry an `OfflinePushRights` refinement with an
independent boolean for each push kind (`flags`, `content`, `add`, `remove`),
defaulting to all-permitted. When `push` is true, the engine SHALL derive a push
of a given kind only when the matching right is permitted; a forbidden push kind
is treated like the read-only path for that kind alone (the local change is kept
pending and never pushed, and a forbidden delete is not applied to the replica),
while other kinds still propagate.

#### Scenario: Flags allowed, deletes forbidden
- GIVEN a source with `push = true`, `rights.flags = true`, `rights.remove = false`
- AND a placement with a locally-changed flag set and another locally tombstoned
- WHEN the collection is synced
- THEN the flag change is pushed
- AND no remove is pushed, and the tombstone is retained (not dropped) for a later sync
