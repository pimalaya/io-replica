---
cairn: change
change: local-add-mutation
---

# Delta

## ADDED Requirements

### Requirement: The offline mutation vocabulary
`ReplicaMutation` SHALL stage a local edit to one collection offline, reconciled
on the next sync: `SetFlags` (replace a placement's flags, mark it dirty),
`Remove` (tombstone), `Edit` (repoint at a new body, resolving a conflict against
the observed remote revision), `Copy` (stage a `Created` in a target carrying the
source origin, so the push is a server-side copy), `Move` (tombstone the source
carrying its destination origin, so the push is one server-side move), and `Add`.
A mutation SHALL touch the local replica only; the remote is reconciled by sync.

### Requirement: Add stages a locally-authored create
`ReplicaMutation::Add { handle, link_id, flags, object, body, meta }` SHALL stage
a brand-new item with no remote origin: a `ReplicaStatus::Created` placement in
the coroutine's collection under the provisional `handle`, at `level = Full`, with
`base = None` and `origin = None`, pointing at `object`; plus a `StoreObject`
carrying `body`. Because the create has no origin, the next sync SHALL push it as
`ReplicaChange::Add { origin: None }` — an append that uploads the body — not a
server-side copy. `Add` SHALL NOT require an existing source placement, and SHALL
fail rather than overwrite when a live (non-tombstone) placement already holds
`link_id`.

## MODIFIED Requirements

## REMOVED Requirements
