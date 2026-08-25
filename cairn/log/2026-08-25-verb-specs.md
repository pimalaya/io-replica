---
cairn: log
change: verb-specs
date: 2026-08-25
---

# Two of the five verbs get a spec, and one of them gets the rule that was already load-bearing

`upgrade` and `rekey` had no capability file. Their rules existed, filed under whichever seam they happened to touch: the hydration ladder's six requirements in `sync.md`, which opens by describing a merge that fetches nothing, and the two a rebuild rests on in `storage.md`, one of them under "a write batch is applied in order".

## What landed

- **`cairn/spec/upgrade.md`**, the detail ladder as its own capability. Six requirements moved out of `sync.md` verbatim: fetch batches are order-independent, a linked body carries its base, a mutable body is fetched rather than linked, a fetch establishes a link only for a not-yet-linked item, an upgrade revisits what it never got, and a fetch never establishes a link the collection already holds.

- **`cairn/spec/rekey.md`**, the rebuild contract. Two requirements moved out of `storage.md` (a fetch refreshes the key at every tier, a key survives a rekey), and four were written that existed nowhere:
  - a rebuild matches by link id, because identity is the only thing a handle-space change leaves intact;
  - an ambiguity survives one, since renumbering two copies of an identity does not merge them;
  - **a rebuild's drops are `Superseded`, and that is what licenses the rebind**;
  - the epoch bump commits with the batch.

- **`ReplicaOpen` gets a requirement in `coroutine.md`**, not a file of its own. It decides nothing: it is the offline read, one `WantsLoad` answered straight back, and it exists so a consumer needs no second code path to project a replica without a network. Saying that is the whole content.

## Why the third rekey rule is the one that mattered

It had already shipped as a bug. A rebuilt handle space froze every item of its collection in the reference store, because the drop-and-upsert pair a rebuild emits is indistinguishable, from the rows alone, from one source reporting an identity under a second handle. The store's duplicate-link-id floor read it as the second and kept the handle the server had just voided.

The rule that separates them is the drop's reason. It existed in `ReplicaDropReason`, io-replica emitted it correctly, io-pimdir did not read it, and no capability file said it was a rule. A capability nobody can find is a capability nobody checks; the fix landed in io-pimdir (`rekey-carries-the-spine`) and the rule is written down here.

## Verification

- No behaviour changed: 54 requirements before, 58 after, none dropped, and every moved one is byte-identical.
- Each of the four new requirements was checked against the code: `rekey.rs:174` emits `Superseded`, `rekey.rs:246` carries `ambiguous_handles` over (with `an_ambiguous_identity_survives_a_handle_space_change` guarding it), `open.rs:47` scopes to `All`.
- 216 tests green, `cargo clippy --all-targets --all-features` clean, `cargo fmt`.

Also landed: the nine change folders still sitting beside `archive/` are archived, as the other nineteen already were.

Capabilities moved: `upgrade` (new), `rekey` (new), `coroutine`, `sync`, `storage`.
