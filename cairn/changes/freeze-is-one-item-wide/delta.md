---
cairn: delta
change: freeze-is-one-item-wide
---

## ADDED Requirements

None. The rule below is folded into the existing "An ambiguous identity derives nothing".

## MODIFIED Requirements

### Requirement: An ambiguous identity derives nothing
Unchanged in what it forbids. It now also states the scope: the freeze is one item wide and SHALL NOT halt anything. A run meeting one SHALL reconcile the rest of the collection normally, in both directions, and SHALL complete; a staged mutation against a frozen placement SHALL be refused as that mutation rather than as the run, so a queued one parks and the drain carries on.

Scoping is what makes deriving nothing an acceptable answer at all: the same rule applied a batch or a collection wide would strand a mailbox on one double delivery, which is worse than the mispairing the freeze exists to prevent.

#### Scenario: A frozen item does not stop its neighbours
- GIVEN a collection holding one frozen identity beside ordinary members
- WHEN it is synced with remote and local changes on both
- THEN the ordinary members pull and push as usual, and the frozen one derives nothing

## REMOVED Requirements

None.
