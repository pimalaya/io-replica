---
cairn: change
change: duplicate-link-id-mints-an-item
---

# Delta

## ADDED Requirements

### Requirement: A second copy of an identity is minted, not withheld
A fetch resolving a placement to a link id another placement of the same collection already carries SHALL give that placement a **minted** link id (pimdir SPEC §9, `dup:<hint>#<handle>`) and link it, rather than leaving it unlinked. The minted form SHALL be derived from the hint and the placement's own handle alone, so it is deterministic: the same collection re-read from scratch mints the same key, and a rebuild carries it rather than re-deriving it.

The engine identifies a placement by its collection and link id, and a source binds one identity with one handle, so two placements cannot share one key. What follows from that is which of the two gets the key, not that one of them must go without: a source holding two resources is holding two items, and an engine that stores one of them is losing data at the point where it noticed the problem.

Minting SHALL be decided against the whole collection rather than the batch, through the load by link ids the upgrade already performs, since a batch hydrating only the second copy would otherwise take the key.

#### Scenario: The second copy is stored
- GIVEN a collection whose placement already holds a link id
- WHEN a fetch resolves another handle of that collection to the same link id
- THEN that placement is linked under a minted key, with its own body, meta and base

#### Scenario: The mint is stable
- GIVEN a collection whose duplicate was minted
- WHEN the same collection is enumerated and hydrated again from an empty store
- THEN the same handle receives the same minted key

### Requirement: A minted identity is an ordinary item
A placement carrying a minted link id SHALL be subject to every rule an ordinary one is: it reconciles in both directions, it is offered to a source that lacks it as a `Created` append, its drop marks the shared item deleted, and it merges and conflicts on the ordinary rules. The engine SHALL NOT read the key's shape, and SHALL derive no rule from it.

Withholding it would mean the engine deciding which of two copies a user is allowed to have on the other side, which is the judgement it does not have. A target that refuses the duplicate says so itself, with a protocol-level refusal (CardDAV `no-uid-conflict`, CalDAV `no-uid-conflict`), and that refusal is a rejected push the consumer reports. Liberal in what is read, strict in what is produced: nothing is invented on the way out, and nothing is silently dropped either.

#### Scenario: Both copies reach the other source
- GIVEN two items of one collection sharing a hint, one keyed bare and one minted
- WHEN a source that holds neither is reconciled
- THEN both are offered as appends, and a refusal of either is reported as a rejected push

## MODIFIED Requirements

### Requirement: A fetch never establishes a link the collection already holds
Applying a fetched item SHALL NOT set a placement's `link_id` to one another placement of the same collection already carries, whether that other placement is in the same batch or only in the store. The engine identifies a placement by its collection and link id, and a source binds it with one handle, so a second placement resolving to the same identity cannot take the key: taking it would overwrite the first binding's handle, and the fact that the source holds two resources would be lost at that write, before any later rule could act on it.

The second placement SHALL instead be linked under a minted key, per the requirement above. The check SHALL be made against the whole collection, not only the placements being upgraded, since a batch hydrating just the second copy would otherwise link it under the key the first already holds.

#### Scenario: A second copy is minted, not linked to the same key
- GIVEN a collection whose placement already holds a link id
- WHEN a fetch resolves another handle of that collection to the same link id
- THEN the first placement keeps its key and its handle, and the second is linked under a minted one

## REMOVED Requirements

### Requirement: An ambiguous identity derives nothing
**Reason**: nothing is ambiguous any more. Two resources are two items, each with its own key, handle and base, so each derives changes on the ordinary rules. The freeze existed because the engine could not tell which copy a change belonged to while both shared one key; they no longer do. Its scoping requirement (one item wide, halting nothing, landed as `freeze-is-one-item-wide`) goes with it, having nothing left to scope.

### Requirement: An ambiguity clears when the source resolves it
**Reason**: there is no state to clear. A duplicate that disappears from a complete snapshot is now an ordinary vanish of an ordinary item, handled by the existing drop rules.

### Requirement: An ambiguous identity is neither propagated nor deleted across sources
**Reason**: both exclusions were consequences of one key being claimed twice. A minted item is propagated and deleted like any other, and a target's own refusal (`no-uid-conflict`) is what stops a duplicate from spreading, rather than the engine pre-empting the decision.

### Requirement: An ambiguity survives a rebuild
**Reason**: a minted key is a key, and `rekey` already carries keys across a handle-space change. Renumbering two copies still does not merge them, because they were never one item.
