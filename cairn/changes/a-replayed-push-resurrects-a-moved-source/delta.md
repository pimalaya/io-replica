---
cairn: delta
change: a-replayed-push-resurrects-a-moved-source
---

## ADDED Requirements

### Requirement: A lost push record can abandon a move
*Folds into sync.md.*

Where a move's staged edit rides ahead of its remove, a crash between the update being serviced and the write recording it SHALL leave the move abandoned rather than half-applied: the next run enumerates a revision the tombstone's base does not name, and an enumerate carries a revision and no body, so the replayed echo is indistinguishable from a remote edit. Edit-beats-delete SHALL then win, replacing the tombstone with a fresh pull, and the member SHALL stay in the source collection, live and clean at the pushed revision.

This is the conservative reading and the only one available: the alternative is deleting content on the strength of a revision the replica cannot account for. Nothing is lost either way. The edit landed, the member is where it started, and the consumer is free to stage the move again.

A move carrying no staged edit is unaffected. Its remove relocates the member, so a lost push record leaves the next enumerate listing nothing for the handle, and the tombstone is dropped as a delete both sides already agree on.

#### Scenario: A crash between a move's edit and its record
- GIVEN a member with a staged content edit, moved into a target
- WHEN the update the move pushes ahead of its remove lands and the write recording it is lost
- THEN the next run reads the revision as a remote edit, the tombstone is replaced by a fresh pull, and the member stays in the source collection

## MODIFIED Requirements

None.

## REMOVED Requirements

None.
