---
cairn: tasks
change: headless-conflict-policy
---

- [x] Add `OfflineConflictPolicy` enum + `conflict` field on `OfflineSyncOptions`
- [x] Wire policy into `reconcile_content`'s conflict branches
- [x] PreferRemote: drop local edit, pull (reuse pull_content path)
- [x] PreferLocal: push the local body as an Update
- [x] KeepBoth: stage a Created duplicate of the local body
- [x] Unit tests per policy on a both-sides-edited placement
- [x] Confirm immutable-content (no revision) never triggers a conflict
- [x] Land: fold delta into spec, append log entry
