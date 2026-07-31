---
cairn: delta
change: object-bytes-by-reference
---

## ADDED Requirements

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

## MODIFIED Requirements

(none)

## REMOVED Requirements

(none)
