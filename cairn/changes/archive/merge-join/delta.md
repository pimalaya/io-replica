---
cairn: delta
change: merge-join
---

## ADDED Requirements

### Requirement: An enumeration is ordered by handle
`ReplicaRemoteSnapshot::items` SHALL be sorted by handle and SHALL list each handle at most once. The merge walks it beside the local placements in that order rather than indexing it, which is what keeps a whole-collection sync from copying both key spaces to join them; protocols hand it over sorted already, an IMAP SEARCH returning ascending UIDs.

The engine SHALL NOT depend on a consumer honouring this: a snapshot that arrives unsorted is sorted, and a handle listed twice is collapsed to its first item, so getting it wrong costs a pass rather than correctness.

#### Scenario: An unordered enumeration
- GIVEN a snapshot whose items arrive in any order
- WHEN the collection is synced
- THEN it derives exactly what the same snapshot sorted derives

### Requirement: The merge joins the two sides rather than copying them
The merge SHALL pair local placements with remote items by walking both in handle order, taking each placement rather than copying it per candidate. A copy SHALL be made only where a write takes ownership of one.

This is a shape requirement rather than a behaviour one, and it is stated because the shape is what the cost is: the candidate set, the order it is merged in and every change derived from it stay exactly as they were, and the property comparing a delta run against a full one is the guard on that.

A delta snapshot keeps its own rule for which of the joined handles is a candidate: the ones it reported changed or vanished, plus every locally non-clean handle, whose pending push it would otherwise never revisit. A never-based one (a staged create) is a candidate with no remote state to merge against, since the enumeration will not mention it until it lands.

### Requirement: A write batch is bounded and cut between candidates
The merge SHALL hand a write batch over once it holds `ReplicaSync::WRITE_CHUNK` writes, rather than holding one write per member until the last candidate is resolved. What this bounds is memory rather than a crash window: a lost batch costs a re-merge, where a lost push costs a round trip.

A batch SHALL be cut between candidates and never inside one. The writes one candidate derives are consistent only together: a keep-both resolution stages the local body as a new member beside the pulled placement it forked from, and a cut between them would lose that body if the next batch never landed.

The checkpoint rule is unchanged and is what makes a partially merged run safe to resume: an intermediate batch carries no checkpoint, so a run interrupted mid-merge re-enumerates from the same cursor and re-derives whatever it had not recorded.

#### Scenario: A merge larger than one batch
- GIVEN a snapshot deriving more writes than one batch holds
- WHEN the collection is synced
- THEN the writes arrive in several batches, and only the last one carries the checkpoint
