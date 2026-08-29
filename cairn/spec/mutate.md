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
- `Edit` — store a new body and repoint the placement at it (full level),
  keeping the base so the next sync derives the push. An edit whose object is
  the one the base already holds stages nothing and SHALL leave the status where
  it found it, `ReplicaPlacement::staged_edit` being the single reading of
  "there is a local content edit here"; every other edit marks the placement
  dirty. Editing a conflicted placement resolves it whatever body it carries,
  the base adopting the remote state observed at conflict time, both halves of
  it (see below).
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

### Requirement: A resolution is measured against the remote it settled
The base an `Edit` resolving a conflict leaves SHALL be the remote state the resolution was merged against: `conflict_revision` as the base revision **and** `conflict_object` as the base object. A conflicted placement holding no base SHALL be given one from the same pair, its own resolution being where the two sides first agree.

Adopting the revision alone leaves the pair contradicting itself, the base claiming a revision its object was never the content of, and the sync's local-side signal is the object: a placement points at a body its base does not hold. A resolution keeping the ancestor of the divergence therefore read as nothing to push, while the adopted revision read as nothing to pull, so the decision never left the machine and the flag pass rebased the divergence away. Keeping the ancestor is the ordinary three-way merge answer, and the resolving tools offer it outright.

The four ways to resolve then fall out of the one comparison: keeping the local body, the ancestor, or a merge of the resolver's own pushes an `Update` gated on the recorded revision, and adopting the remote body wholesale owes no push and settles clean on the next run. The base is also the ancestor a later conflict is merged against, which is right for the same reason: after a resolution, the last state the two sides shared is the remote state the decision was taken against.

#### Scenario: Keeping the ancestor
- GIVEN a conflicted placement resolved with the body its base holds
- WHEN the collection is synced
- THEN an `Update` carrying that body is pushed, gated on the revision recorded at conflict time

#### Scenario: Taking the remote body
- GIVEN a conflicted placement resolved with the recorded diverging body
- WHEN the collection is synced
- THEN nothing is pushed and the placement lands clean

#### Scenario: A resolution with no base
- GIVEN a create-collision conflict, which has no base
- WHEN it is resolved with an edit
- THEN the placement is based on the recorded revision and body, and the next sync pushes the resolution instead of re-marking the conflict
