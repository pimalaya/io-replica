---
cairn: delta
change: write-batch-order
---

## ADDED Requirements

### Requirement: A write batch is applied in order
`ReplicaStorage::write` SHALL apply the ops in the order they are listed, and atomically. Ordering is what a batch naming one handle twice rests on, which the engine does emit: a sync that clears an ambiguity and then reads the same handle as vanished writes the cleared placement and then drops it, and the drop is the answer.

A storage MAY NOT group the batch by op kind, or otherwise reorder it: what looks like an independent set of rows is not one.

The engine SHALL NOT rely on the order where it can avoid emitting the pair at all. A rebuild in particular drops only the old handles no upsert of the same batch writes: a new handle space commonly reuses an old handle, and an unmatched staged edit is resurrected under the handle it already had, so the collision there is between ops far apart in a long list, and the reasoning that they are safe lives in neither of them.

#### Scenario: A rebuilt spine reuses a handle
- GIVEN a placement the old handle space held under a handle the new one reuses
- WHEN the collection is rebuilt
- THEN the batch writes that handle once, and does not also drop it
