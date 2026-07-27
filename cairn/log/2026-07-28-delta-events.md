---
cairn: log
change: delta-events
landed: 2026-07-28
---

# Per-item delta events (DELTA_PLAN D1)

Added `ReplicaEvent` (`Added` / `FlagsChanged` / `ContentChanged` / `Vanished` /
`Conflicted` / `Created`, each carrying a handle) and a `ReplicaSyncReport.events`
vector. The merge and the push-confirmation phase now emit an event at every
per-item outcome, paired with the counter increment it already made, so the
counters summarise the events. `ReplicaSyncReport` lost `Copy` (it now holds a
`Vec`); the coroutine returns it with `mem::take`. Events are the outbound half
of the D1 seam — data, spine level, no I/O — that hooks and watch will ride;
hook *execution* stays in the consumer (D2/D3).

Five unit tests added (Added on remote-add, FlagsChanged on remote-pull and on
accepted-push, Vanished on delta-vanish, Created on accepted-create under the
server-assigned handle). All 107 sync unit tests plus integration and property
suites pass; fmt and clippy clean. Consumers are unaffected: they read the
report's counter fields and simply do not surface `events` yet.

Spec updated: `sync` (ADDED: Per-item delta events).
