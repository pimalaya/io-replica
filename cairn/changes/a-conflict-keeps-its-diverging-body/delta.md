---
cairn: delta
change: a-conflict-keeps-its-diverging-body
---

## ADDED Requirements

### Requirement: A conflict keeps the body it diverged from
*Folds into sync.md.*

A placement marked conflicted SHALL carry `conflict_object`, the remote body at the revision `conflict_revision` names, so the divergence can be read without asking the remote for it. Both SHALL be set together, cleared together on resolution, and dropped together when the tracked revision moves: a body that outlives the revision recorded beside it describes a version the server no longer holds, and a resolver trusting it would merge against a phantom.

The engine fetches nothing, so the body is requested rather than taken: marking a conflict marks the body wanted and the upgrade pass supplies it. A conflict whose body has not yet landed is visible and unresolvable, as a probed placement holding no body is visible and unreadable.

Storing it is what lets resolution leave the process that found it. A resolver holding base, local and remote needs no credentials, no backend and no network, and a conflict between two hand-edited bodies is decided by a human long after the run that found it.

#### Scenario: The diverging body is stored, not refetched by the resolver
- GIVEN a based placement edited locally and changed on the remote, `conflict = Manual`
- WHEN the collection is synced and the wanted body is supplied
- THEN the placement holds the remote body beside the observed revision, and base, local and remote are all readable from the store

#### Scenario: A remote that moves again invalidates the stored body
- GIVEN an unresolved conflict whose stored body matches its recorded revision
- WHEN a later sync observes a newer remote revision
- THEN the recorded revision advances and the stored body is dropped in the same write

### Requirement: An upgrade supplies a conflict's diverging body
*Folds into upgrade.md.*

An upgrade SHALL revisit a conflicted placement that holds no `conflict_object`, and SHALL apply the fetched body to `conflict_object` rather than to the placement's own object. The two are different questions about the same handle: the placement's object is what the local side holds, and the conflict object is what the remote holds instead, so a fetch answering one SHALL NOT be read as answering the other.

#### Scenario: A conflicted placement without its diverging body
- GIVEN a conflicted placement holding a local body and no conflict object
- WHEN it is upgraded
- THEN the fetched body lands as the conflict object and the local body is untouched

## MODIFIED Requirements

### Requirement: Headless conflict resolution
Unchanged in what each policy does. `Manual` now also records the diverging remote body as `conflict_object` beside `conflict_revision`, so waiting for the consumer's edit does not oblige the consumer to fetch. `PreferLocal`, `PreferRemote` and `KeepBoth` decide within the run and record neither.

## REMOVED Requirements

None.
