---
cairn: change
id: a-replayed-push-resurrects-a-moved-source
status: landed
created: 2026-08-29
---

# A replayed push resurrects a moved source

## Why

The property model asserts that a move intent it did not mark voided ends with its link id in the target collection or in the source one, and `a_crashed_write_never_loses_data` breaks it at a case count the default run never reaches. The saved seed shrinks to five operations: a local edit on a member, a move of that member into the archive, two syncs with the crash injected at the fourth write batch, and a local delete of the member.

The engine is not at fault, and the evidence is the case itself. Take the same operations, the same injected crash, and drop only the trailing delete: the model passes at every crash point. The engine leaves the item in the inbox, alive and clean, with the edited body on the server. Nothing is lost by the engine, and no earlier crash point loses it either.

What happens is the documented interaction of two engine rules. A move whose source carries a staged edit pushes the edit ahead of the remove, so the relocated member carries the edited body and the remove derives again on the next run once the base holds what was pushed. The crash falls between that update being serviced and the write recording it, which is the window the at-least-once contract exists for. The next run enumerates a revision the tombstone's base does not name, and an enumerate reports a revision and no body, so the replica cannot tell its own replayed echo from a stranger's edit. Edit-beats-delete then does what it promises: it refuses to delete content it has not seen, replaces the tombstone with a fresh pull, and the move is abandoned with the item back in the inbox. That is the conservative answer, and it is the only one available from a revision alone.

The user then deletes that resurrected member. A strictly later explicit delete of the same item supersedes the move it displaced, which is exactly the rule the ledger states for itself. The ledger already applies it on both the other handle-keyed axes, dropping the edit and flag claims a local delete supersedes, and simply forgets the move claim. So the invariant asserted is stronger than the contract, and the model is what is wrong.

## What

Void a staged move when a later local delete removes the source it was staged on, alongside the edit and flag claims that delete already voids. A move's source is tombstoned by the move itself and cannot be picked again while it stays that way, so the only way this fires is on a source the engine put back, which is precisely the case where the move is no longer owed.

The spec gains the engine behaviour that makes the model justifiable, which is current truth and not a behaviour change: a lost push record can abandon a move, and the item stays in its source collection rather than reaching the target.

No production code moves, and the saved seed stays pinned.
