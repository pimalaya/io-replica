---
cairn: change
id: delta-events
status: active
created: 2026-07-28
---

# Per-item delta events (DELTA_PLAN D1)

## Why
`sync` today surfaces its decisions only as `OfflineWriteOp`s and report counters.
Watch/notify/log hooks — and richer reporting in consumers like neverest — need
*per-item events* (this handle was added / flags changed / content changed /
vanished / conflicted / created), as data yielded by the coroutine, no I/O in the
core. This is the outbound half of the D1 signal-and-event seam.

## What
Accumulate a `Vec<OfflineEvent>` during the merge (one variant per per-item
outcome, carrying the handle and, where relevant, the link id) and expose it on
`OfflineSyncReport`. The existing counters become a fold over the events so
nothing is counted twice. Hook *execution* stays in the consumer (D2/D3); the
engine only emits events.
