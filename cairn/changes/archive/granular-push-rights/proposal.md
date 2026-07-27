---
cairn: change
id: granular-push-rights
status: landed
created: 2026-07-28
---

# Granular push rights

## Why
`OfflineSyncOptions.push` is all-or-nothing: a source is either fully writable or
fully read-only. The policy-driven replica (neverest's managed-replica model)
needs a source that can, say, accept flag changes but refuse deletes, or accept
creates but not content updates. This is the *authority* axis of a per-source
policy, and it must be enforced in the merge, not faked after the fact (a
forbidden op must never be derived, so it never loops as a rejection).

## What
Add an `OfflinePushRights { flags, content, add, remove }` refinement to
`OfflineSyncOptions`. `push` stays the master switch (false = read-only, rights
ignored); when `push` is true, each right gates its push kind at the merge site.
A forbidden op behaves like the read-only path for that op only: the local change
is kept pending (dirty / tombstone / provisional) and never pushed, and a
forbidden delete is *not* applied to the replica either. Default rights are all
true, so existing behaviour is unchanged.
