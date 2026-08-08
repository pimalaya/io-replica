---
cairn: spec
capability: mutate
status: current
---

# Mutate

`ReplicaMutate` is the I/O-free coroutine that applies a local edit to one
collection offline, with no network. It loads the target placement, stages the
resulting `ReplicaWriteOp`s, and lets the driver write them; the remote is
reconciled by the next `sync`. This is the write half of the "generic in the
data, disciplined in the writes" rule: a client never writes storage rows
directly — it stages a `ReplicaMutation`, so the sync always knows what changed.

### Requirement: The offline mutation vocabulary
`ReplicaMutation` SHALL stage a local edit to one collection offline, reconciled
on the next sync:

- `SetFlags` — replace a placement's flags and mark it dirty (a pending create
  stays `Created`, an unresolved conflict stays `Conflict`; the flag change rides
  along).
- `Remove` — tombstone a placement, kept until synced. Absorbed as a staged
  delete (the item is marked deleted, its binding kept), so the next sync pushes
  the remove.
- `Edit` — store a new body and repoint the placement at it (full level, dirty),
  keeping the base so the next sync derives the push; editing a conflicted
  placement resolves it, the base adopting the remote revision observed at
  conflict time.
- `Copy` — stage a `Created` placement in a target under a caller-supplied
  `placeholder`, carrying the source origin; the source is untouched.
- `Move` — stage a `Created` placement in the target under a caller-supplied
  `placeholder` (carrying the source origin), **and** tombstone the source. A
  move is thus a copy into the target plus a remove from the source, both derived
  on the next sync; the source's tombstone and the target's create land in their
  respective collection hubs.
- `Add` — see below.

A mutation SHALL touch the local replica only; the remote is reconciled by sync.

### Requirement: Add stages a locally-authored create
`ReplicaMutation::Add { handle, link_id, flags, object, body, meta }` SHALL stage
a brand-new item with no remote origin (compose, import): a `ReplicaStatus::Created`
placement in the coroutine's collection under the provisional `handle`, at
`level = Full`, with `base = None` and `origin = None`, pointing at `object`; plus
a `StoreObject` carrying `body`. Because the create has no origin, the next sync
SHALL push it as `ReplicaChange::Add { origin: None }` — an append that uploads
the body — not a server-side copy. `Add` SHALL NOT require an existing source
placement, and SHALL fail (`ReplicaMutateError::LinkExists`) rather than overwrite
when a live (non-tombstone) placement already holds `link_id`; a tombstoned
`link_id` does not block the create.

> Seed spec (Cairn, 2026-08-01): captures the offline mutation vocabulary,
> retro-documented when `Add` was added.

### Requirement: A mutation may restate the sort key
`Add` SHALL carry a sort key, and `Edit` SHALL carry an optional one on the same
terms as its optional summary: absent keeps the stored key. An edit that changes
what the key is derived from has to say so, or the item stays where it was in
the list.
