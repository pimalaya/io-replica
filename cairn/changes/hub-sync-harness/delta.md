---
cairn: delta
change: hub-sync-harness
---

## ADDED Requirements

### Requirement: A hub-backed store owns the rows the hub cannot key
`ReplicaHub::absorb` SHALL ignore an upserted placement carrying no link id, because the hub keys items by link id and has nowhere to put one. Every row a sync pulls is such a placement: an enumeration yields handles, and the link id lands on the first meta fetch.

A hub-backed storage SHALL therefore hold those rows itself and return them from `load` beside the projection, until a fetch resolves their identity and the hub takes them over. A storage that does not is not a partial mirror but a broken one: its replica forgets every member it pulls, and an incremental enumeration never lists them again.

It follows that mirroring is a sync **plus** an upgrade. The hub offers a member to a source that lacks it only when it holds the body, so a consumer that never hydrates never mirrors anything.

#### Scenario: A pulled member reaches the other source
- GIVEN two sources over one hub, one holding a member the other lacks
- WHEN the holder is synced and its rows hydrated
- THEN the hub offers the member to the other source, which appends it
