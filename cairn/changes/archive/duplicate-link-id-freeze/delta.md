---
cairn: change
change: duplicate-link-id-freeze
---

# Delta

## ADDED Requirements

### Requirement: A fetch never establishes a link the collection already holds
Applying a fetched item SHALL NOT set a placement's `link_id` to one another
placement of the same collection already carries. The engine identifies a
placement by `(collection, link_id)` and a source binds it with one handle, so a
second placement resolving to the same identity has nowhere to live: linking it
overwrites the first binding's handle, and the fact that the source holds the
identity twice is lost at that write, before any later rule can act on it.

The losing handle SHALL instead be recorded on the binding that holds the
identity, so the ambiguity survives the round trip through the storage. That is
what makes the freeze below **sticky**, which it must be: the twin appears in
exactly one enumeration, the one that discovers it, and an incremental
enumeration never mentions it again.

### Requirement: An ambiguous identity derives nothing
A placement whose binding carries ambiguous handles SHALL project
`ReplicaStatus::Ambiguous`, and the engine SHALL derive no change for it in
either direction while it does:

- no push of any kind, on any axis;
- **no vanish-delete**: its absence from a complete snapshot SHALL NOT be read
  as the item being gone from that source, since the source demonstrably holds
  another copy of the identity;
- no cross-source pairing or propagation in the hub, and no `Created` append to
  a source that lacks it;
- no staged mutation against it;
- a rekey SHALL carry the state over a handle-space change untouched.

An enumeration reporting the identity once again SHALL clear the ambiguous
handles, and the item resumes syncing with no further ceremony.

The reason the rule is stated as *derive nothing* rather than *pick a copy*: the
engine has no basis for choosing which copy a change belongs to, and choosing
wrongly destroys mail. A frozen item is mirrored zero times rather than once,
which is the cost of not guessing.

#### Scenario: A second copy is recorded, not linked
- GIVEN a collection whose placement already holds a link id
- WHEN a fetch resolves another handle of that collection to the same link id
- THEN the second placement stays unlinked, and the first binding records the second handle as ambiguous

#### Scenario: An ambiguous placement is never deleted by a vanish
- GIVEN an ambiguous placement bound to a source
- WHEN a complete snapshot of that source omits its handle
- THEN no delete is derived, on that source or on any other holding the identity

#### Scenario: Resolving the duplicate resumes the sync
- GIVEN an ambiguous placement
- WHEN an enumeration reports the identity under a single handle
- THEN the ambiguous handles are cleared and the placement reconciles normally

## MODIFIED Requirements

## REMOVED Requirements
