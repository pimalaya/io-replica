---
cairn: tasks
change: granular-push-rights
---

- [x] Add `OfflinePushRights` (flags/content/add/remove, Default all true) to sync.rs
- [x] Add `rights` field to `OfflineSyncOptions`; keep `push` as master switch
- [x] Gate the flag push in `reconcile_flags` on `rights.flags`
- [x] Gate the content Update push in `reconcile_content` on `rights.content`
- [x] Gate the `Add` pushes (created + resurrected) on `rights.add`
- [x] Gate the `Remove` push (tombstone) on `rights.remove`; keep the tombstone pending, do not drop
- [x] Update existing `OfflineSyncOptions` literals in tests
- [x] Add unit tests: each right suppresses its op while the others still fire
- [x] Land: fold delta into spec/sync.md, append log entry
