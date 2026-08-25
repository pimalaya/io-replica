---
cairn: change
id: verb-specs
status: landed
created: 2026-08-25
---

# Two of the five verbs had no spec, and one of them had just shipped a bug

## Why

The crate has five coroutines. Four of them decide something, and `cairn/spec/` held capability files for two: `sync` and `mutate`, beside the seams (`storage`, `hub`) and the contract (`coroutine`). `upgrade` and `rekey` had none.

Their rules were not missing, they were **filed under the seam they happen to touch**. The hydration ladder's six requirements sat in `sync.md`, which opens by describing a three-way merge that fetches nothing; the two rules a rebuild rests on sat in `storage.md`, one of them under the heading "a write batch is applied in order". A reader asking what a rekey *is* found an ordering caveat and no contract.

That is not only untidy. On 2026-08-25 a rebuilt handle space was found to freeze every item of its collection in the reference store, because a rebuild's drop and a duplicated identity produce the same diff and nothing said which is which. The rule that separates them (the drop's reason) existed in the code, was relied on by two repositories, and was written down in neither. A capability nobody can find is a capability nobody checks.

## What

- `cairn/spec/upgrade.md`: the detail ladder as its own capability, taking the six fetch and identity requirements out of `sync.md` verbatim.
- `cairn/spec/rekey.md`: the rebuild contract, taking the two key requirements out of `storage.md` and stating four rules that were nowhere: what a rebuild matches on, that an ambiguity survives one, that its drops are `Superseded` and what that licenses, and that the epoch bump commits with it.
- `ReplicaOpen` gets a requirement in `coroutine.md` rather than a file: it decides nothing, and saying so is the point.
- The nine landed change folders still sitting beside `archive/` are archived, as the other nineteen already were.

No behaviour changes. Every requirement moved is moved verbatim; the four new ones state what the code already does, and each is true of it today.
