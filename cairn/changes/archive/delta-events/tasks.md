---
cairn: tasks
change: delta-events
---

- [x] Add `OfflineEvent` enum (Added / FlagsChanged / ContentChanged / Vanished / Conflicted / Created), per handle
- [x] Accumulate events at each merge branch in sync.rs
- [x] Expose `events` on `OfflineSyncReport`; derive counters from events
- [x] Unit tests: each branch emits its event; counters equal the fold
- [x] Land: fold delta into spec, append log entry
