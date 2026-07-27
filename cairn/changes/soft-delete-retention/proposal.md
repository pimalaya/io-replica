---
cairn: change
id: soft-delete-retention
status: draft
created: 2026-07-28
---

# Soft-delete retention (backup)

## Why
Backup needs the replica to *keep* a message the source expunged, not drop it —
the whole point of a backup is that a remote delete never loses the local copy.

## What
This is realised at the **storage seam**, not the merge core: the engine already
signals a remote delete as `OfflineWriteOp::DropPlacement`, and a backup storage
MAY treat that drop as a soft-delete (retain the row, mark it removed-upstream)
rather than a hard delete, as long as it hides soft-deleted rows from `load` so
the merge does not re-derive against them. So the engine work here is a **spec
clarification** that a storage hiding rows from `load` is safe and that
`DropPlacement` is the retention decision point; no merge-core change. (If a
consumer prefers engine-side retention, that becomes a separate `on_remote_delete`
sync option — deferred unless a consumer needs it.)
