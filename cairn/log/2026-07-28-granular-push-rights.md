---
cairn: log
change: granular-push-rights
landed: 2026-07-28
---

# Granular push rights

Added `OfflinePushRights { flags, content, add, remove }` (all permitted by
default) as a refinement of `OfflineSyncOptions.push`, with a private
`OfflineSyncOptions::may_push` helper. When `push` is true, each merge site now
gates its push kind on the matching right: the flag push in `reconcile_flags`,
the content `Update` in `reconcile_content` and the tombstone's edit-ahead-of-move
update, the `Add` pushes (both the `Created` and the resurrected-on-remote-delete
paths), and the tombstone `Remove`. A forbidden kind is kept pending like the
read-only path for that kind alone; a forbidden delete is *not* dropped from the
replica. Default behaviour is unchanged.

Four unit tests added (flags/remove/add forbidden, and flags-allowed-remove-
forbidden pushing only the flag change). All 102 sync unit tests plus the
integration and property suites pass.

Spec updated: `sync` (MODIFIED: Push direction; ADDED: Granular push rights).
This is the first engine step of the policy-driven replica (the *authority* axis
of a per-source policy).
