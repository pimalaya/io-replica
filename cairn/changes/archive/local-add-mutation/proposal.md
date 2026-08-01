---
cairn: change
id: local-add-mutation
status: landed
created: 2026-08-01
---

# Local `Add` mutation (compose / import)

## Why

The offline mutation vocabulary (`ReplicaMutation`) covers editing existing
items — `SetFlags`, `Remove`, `Edit`, `Copy`, `Move` — but has no way to create a
**brand-new, locally-authored** item with no remote origin (compose a message,
import one). Every mutation today reads an existing source placement by handle;
there is no "there is no source, make one" path.

This is the one missing verb for a client to use the store as a writable local
cache (LOCAL_STORE_PLAN §4.3 / action plan M2): a Himalaya `add_message`
(append to a mailbox, e.g. Sent) must stage a create the next sync pushes.

The sync push path already supports the target shape: a `Created` placement with
no base and **no origin** pushes as `ReplicaChange::Add { origin: None, .. }`, an
**APPEND** that re-uploads the body (vs `origin: Some` → a server-side COPY, which
`Copy` uses). So this change only needs to *stage* that shape; the reconcile side
is unchanged.

## What

- Add `ReplicaMutation::Add { handle, link_id, flags, object, body, meta }`:
  - `handle` is the provisional local handle the create is staged under (the
    sync rekeys it to the server-assigned handle on push, exactly as for `Copy`).
  - stages a `ReplicaStatus::Created` placement in the coroutine's collection,
    `level = Full`, `base = None` (no prior sync), `origin = None` (locally
    authored, an APPEND not a COPY), pointing at the stored object.
  - emits a `StoreObject { body: Some(..) }` for the new body.
- The `Add` path does not require finding an existing source placement; it still
  loads the collection to **guard against a link-id collision** with a live
  (non-tombstone) placement, which is a `ReplicaMutateError` rather than a silent
  overwrite.

## Scope / non-goals

- No reconcile/push change — `Add` reuses the existing base-less-`Created` →
  `ReplicaChange::Add` append path.
- No hard-delete verb — `Remove` (tombstone) and `Move`-to-trash already cover
  deletion.
- Capture the offline-mutation vocabulary in a new `cairn/spec/mutate.md` (there
  is no mutate spec today); this change seeds it and adds the `Add` requirement.
