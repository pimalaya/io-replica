---
cairn: log
change: soft-delete-retention
landed: 2026-07-28
---

# Soft-delete retention (backup)

No merge-core change: retention lives at the storage seam. The engine already
signals every removal as a `ReplicaWriteOp::DropPlacement`, and a backup storage
may treat that as a soft-delete — retain the row, marked removed-upstream, and
hide it from `load` — instead of a hard delete. Because the merge reconciles only
what `load` returns, the hidden row is invisible on every later sync (delta or
full), so it is never re-derived and the retained copy survives. Concluded that
an engine-side `on_remote_delete` option is not warranted; the storage owns the
decision.

Added `tests/soft_delete.rs`: a `SoftDeleteStorage` reference (soft-delete on
`DropPlacement`, hide from `load`) driving a seed → sync → server-expunge → sync
flow, asserting the row is retained but hidden and that a subsequent delta *and*
full sync are both quiescent (`ReplicaSyncReport::default()`). Test green; fmt and
clippy clean.

Spec updated: `storage` (ADDED: DropPlacement is the retention decision point;
Hiding rows from load is safe) — a new capability file.
