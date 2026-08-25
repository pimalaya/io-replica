---
cairn: delta
change: delete-disposition
---

## ADDED Requirements

### Requirement: A refused delete follows one policy
`ReplicaSyncOptions` SHALL carry a `ReplicaDeletePolicy`, consulted wherever a local delete cannot go: `push` is false, `rights.remove` is false, or a move's staged edit cannot ride ahead of it (the move must not go without the edit, or the relocated member would carry the body the edit replaced).

`Revert` SHALL undo the delete, landing the placement `Clean` with whatever it had cached. `Keep` SHALL hold the tombstone as it is, so a later run that may push derives the remove again. Either way the engine SHALL derive no push and SHALL NOT apply the delete to the replica.

`Revert` is the default. A held tombstone hides a member the source still holds, and hides it for good: an incremental enumeration never lists an untouched member again, so nothing brings it back. Holding is right only when the refusal is a policy that may lift, which is the consumer's knowledge, not the engine's.

Deletion is the only axis needing this. A refused flag or content change stays dirty and re-derives every run, but a refused delete has to be either undone or held.

A source bound to a hub SHALL be given `Keep`. Reverting states that this source still holds the member, which the hub reads as the item being alive (add-beats-delete across sources): the deletion is cleared for every source and the item is mirrored back to the one it was deleted on. Both readings are coherent, and only the consumer knows which it means.

#### Scenario: The two refusals agree
- GIVEN a tombstoned placement the source still holds
- WHEN it is synced with `push = false`, and again with `rights.remove = false`
- THEN both follow the same policy: reverted by default, held under `Keep`
