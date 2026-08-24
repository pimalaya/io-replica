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

### Requirement: An unread flag set is unknown rather than empty
`ReplicaFlags` SHALL carry an `Unknown` state distinct from a known-empty set,
since the reference storage records the two apart (pimdir SPEC §13: a `NULL`
flags column means never read, `'[]'` means known to carry none). Known-empty
SHALL remain the default, so unknown is stated by a source that read no markers
rather than fallen back to by an ordinary write.

Only a local placement is ever unknown in practice: a source that reports an
item reports what it read. The engine SHALL resolve the state on the first side
that carries a set.

### Requirement: An unknown side holds no opinion in the merge
The flag merge SHALL treat an unknown side as neither an addition nor a removal:
the result is the other side's set, two unknown sides stay unknown, and an
unknown base is the same fact as no base on the flag axis, so nothing is derived
from it and both sides' markers are kept.

Reading unknown as empty would make it an opinion, since element-wise an empty
set says every flag the other side holds was removed here.

#### Scenario: A probed placement learns its markers
- GIVEN a local placement whose flag set is unknown
- AND a source reporting that item with a marker set
- WHEN the collection is synced
- THEN the placement adopts the reported set and nothing is pushed

### Requirement: An unknown set never erases a known one
Absorbing an upsert whose flag set is unknown SHALL leave the shared set alone,
on the same terms as an absent summary and an unknown sort key. A known set,
empty or not, SHALL replace another known set: only unknown is inert.
