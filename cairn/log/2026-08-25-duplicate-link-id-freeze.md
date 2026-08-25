---
cairn: log
change: duplicate-link-id-freeze
date: 2026-08-25
---

# An identity a collection holds twice is frozen, not guessed

The engine's model has nowhere to put a second copy of one identity: a placement is identified by its collection and link id, and a source binds it with one handle. Reproduced against two Stalwart servers with one copy on A and two on B, the consequence was remote data loss on a side the user never touched, in three steps: the first sync paired A's copy with one of B's and left the other invisible; deleting the bound copy on B propagated a delete that removed the only copy on A while B still held the message; and nothing more happened while B kept its QRESYNC checkpoint, until losing it revived the retained row and re-appended the message to A days later.

This is the io-replica half: the invariant, the detection and the rules. io-pimdir persists it next, then neverest reports it end to end.

## What landed

- **`ReplicaStatus::Ambiguous`, `ReplicaPlacement::ambiguous_handles`, `ReplicaSourceBinding::ambiguous_handles`** (capabilities `sync`, `hub`). A status variant rather than a loose flag, deliberately: the enum is matched in `sync`, `hub`, `mutate` and `rekey`, so the compiler forced every rule to say what it does with an identity the engine cannot resolve, which is exactly the omission that produced the loss. The handles sit on the binding because "this source holds it twice" is a per-source fact: in a two-sided store one side may hold the duplicate while the other syncs normally.

- **Detection where identity is assigned** (capability `sync`). `ReplicaUpgrade` already carried "a fetch establishes a link only for a not-yet-linked item"; its sibling now sits beside it: nor one another placement of the collection already holds. The losing handle is recorded on the holder instead, which is what makes the freeze sticky, and it has to be sticky because the twin appears in exactly one enumeration.

  Two things the proposal did not spell out and the tests forced. Both copies of a duplicate are commonly fetched in the same batch and neither is linked yet, so checking against the stored rows alone sees nothing and links both: the check tracks what the batch itself has claimed as well. And a batch hydrating only the second copy cannot see the holder at all under a handle-scoped load, so a fetch that would establish a fresh identity now asks one scoped read (`ReplicaLoadScope::Links`) before writing. That read is asked only when a fetch actually resolves a new identity.

- **The derive-nothing rules** (capabilities `sync`, `hub`). No push on any axis; no vanish-delete, so a complete snapshot omitting the handle is not read as the item being gone; no `Created` append to a source that lacks the identity; no cross-source delete propagation; `mutate` refuses with `ReplicaMutateError::Ambiguous`; `rekey` carries the state over, renumbering the copies not being merging them.

- **Clearing** (capability `sync`). The engine never sees an identity in an enumeration, only handles, so "the source reports the identity once again" reduces to: a complete snapshot that omits a recorded handle, or a delta that reports it vanished. A delta that merely does not mention it says nothing and clears nothing. The last one going lands the placement `Clean` and reconciles it in that same run, so what it slept through is picked up rather than waiting for an enumeration that may never list it again.

## Not in scope, as proposed

A frozen item is mirrored zero times rather than once, which is right for propagation and a regression for a backup; only 1:N bindings serve that, and this change does not pretend to. Bindings stay 1:1. A per-copy link id stays out: folding the handle into the link id makes it server-local, both sides then append to each other, and a duplicate becomes an unbounded duplication loop.

## Verification

- 187 tests green (162 lib, 25 integration), `cargo clippy --all-targets` clean, `cargo fmt`.
- `tests/duplicate_link_id.rs` encodes the reproduction directly: the second copy is recorded rather than linked; an ambiguous placement is never deleted by a vanish; it derives no push; a mutation against it is refused; the freeze survives runs that never mention the twin (step 3 of the reproduction); and resolving the duplicate resumes the sync.
- Hub tests cover the per-source projection and the blocked cross-source delete; a rekey test covers the carry over a handle-space change.

Capabilities moved: `sync`, `hub`.
