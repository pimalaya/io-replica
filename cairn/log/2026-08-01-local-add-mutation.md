---
cairn: log
change: local-add-mutation
landed: 2026-08-01
---

# Local `Add` mutation (compose / import)

Added `ReplicaMutation::Add { handle, link_id, flags, object, body, meta }`, the
one missing offline verb: creating a brand-new, locally-authored item with no
remote origin (compose, import), for a client using the store as a writable
cache (action plan M2).

`mutate.rs`: `ReplicaMutation::handle()` now returns `Option` (Add has no source
handle); the `PendingLoad` state routes Add through a new `create_writes` that
stages a `Created` placement (`base: None`, `origin: None`, `level: Full`) plus a
body-carrying `StoreObject`, after guarding against a live-`link_id` collision
(`ReplicaMutateError::LinkExists`; a tombstoned link id does not block). No
reconcile change was needed — a base-less `Created` with no origin already pushes
as `ReplicaChange::Add { origin: None }` (an append that uploads the body, vs a
`Copy`'s origin-carrying server-side copy), the path sync already tests.

Verified: three new `mutate` unit tests — the append shape (Created/no-base/no-
origin/object/body), the collision guard, and tombstone-link-id allowance; full
suite green (128 unit + integration), fmt + clippy clean.

Spec: created `cairn/spec/mutate.md` (there was no mutate spec) capturing the
offline mutation vocabulary and the new `Add` requirement.
