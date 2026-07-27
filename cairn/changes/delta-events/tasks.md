---
cairn: tasks
change: delta-events
---

- [ ] Add `OfflineEvent` enum (Added / FlagsChanged / ContentChanged / Vanished / Conflicted / Created), per handle
- [ ] Accumulate events at each merge branch in sync.rs
- [ ] Expose `events` on `OfflineSyncReport`; derive counters from events
- [ ] Unit tests: each branch emits its event; counters equal the fold
- [ ] Land: fold delta into spec, append log entry
