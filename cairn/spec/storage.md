---
cairn: spec
capability: storage
status: current
---

# Storage

The storage seam is the local index plus blob store the engine reconciles
against. The consumer implements it (a sqlite index plus a blob dir in the
reference driver); the engine reads it through `load` and `lookup_objects` and
mutates it through a `write` batch of [`ReplicaWriteOp`]s. The engine performs no
I/O of its own: storage effects travel as coroutine yields.

### Requirement: DropPlacement is the retention decision point
The engine SHALL signal every removal — a local delete confirmed by the remote,
or a member vanished upstream — as a `ReplicaWriteOp::DropPlacement`, and SHALL
make no other assumption about what the storage does with it. A storage MAY hard-
delete the row, or soft-delete it (retain the row, marked removed-upstream) for a
backup that must never lose a copy the source expunged. No merge-core option
governs this; retention is the storage's choice.

### Requirement: Hiding rows from load is safe
The merge reconciles only the placements a `load` returns. A storage that hides
soft-deleted rows from `load` SHALL therefore not cause the engine to re-derive
against them: the hidden row is invisible on every later sync, delta or full, so
it is neither re-added nor looped over, and the retained copy survives.

#### Scenario: A backup keeps a remote expunge
- GIVEN a storage that soft-deletes on `DropPlacement` and hides such rows from `load`
- AND a member the remote expunges after a first sync
- WHEN the collection is synced again, whether delta or full
- THEN the row is retained but absent from `load`, and no re-derivation occurs

### Requirement: Object write carries bytes or a reference
`ReplicaWriteOp::StoreObject` SHALL carry either the object's bytes or a
reference to an object the consumer has already persisted to the blob store. The
engine SHALL index the object from its `(hash, size)` in both cases, and SHALL
have the storage write bytes only when they are present. Refcounting, dedup and
garbage collection are unaffected.

### Requirement: A Full fetch may persist its own body
A consumer servicing a `ReplicaTier::Full` fetch MAY stream the body straight
into the blob store and report the object by `(hash, size)` with no inline bytes,
so no full body is held in memory. The engine SHALL treat such a report as an
already-stored object and emit a byteless `StoreObject`. `lookup_objects` — which
lets the engine skip a Full fetch whose object already exists — is unchanged, so
a persist-during-fetch stays idempotent under retry.

#### Scenario: A large body transfers without full materialisation
- GIVEN a source whose Full fetch streams the body into the blob store and reports it by (hash, size)
- WHEN the item is hydrated and later appended to another source
- THEN the engine holds no full body, indexes the object from its (hash, size), and the append streams from the stored blob

### Requirement: A placement carries a presentation sort key
`ReplicaPlacement` and `ReplicaFetchedItem` SHALL carry a sort key: the item's
position in its collection's natural order, derived by the consumer wherever the
summary is derived. The engine SHALL treat it exactly as it treats the summary,
as an opaque value it ferries and never parses.

Empty SHALL mean unknown, and SHALL be the default, so an item is orderable from
the moment it exists and no consumer has to invent a value. It is a plain value
rather than an option because that is what the reference storage records, and
one representation of "not known yet" is less to get wrong than two.

### Requirement: A fetch refreshes the key at every tier
An upgrade SHALL adopt the key from the fetched item at both tiers, unlike the
link id, which is kept once resolved. The key is a projection of content rather
than an identity, so the later and better-informed derivation wins: a full body
carries the real date where an envelope may have carried none.

### Requirement: A key survives a rekey
Rebuilding a collection onto a new handle space SHALL carry each placement's key
over, preferring the one the rekey's meta fetch resolved and falling back to the
key the old placement held, so a handle-space change does not un-sort a
collection.
