---
cairn: log
date: 2026-08-25
change: engine-compaction
---

# The audit's last three repetitions

`engine-algorithm-audit` left a compaction list whose correctness half had already landed. Three items were left, none of them a bug, all of them the same thing written more than once.

## One error for a broken contract

`ReplicaOpenError`, `ReplicaUpgradeError`, `ReplicaRekeyError` and `ReplicaSyncError` were byte-identical but for the verb name in the message, and `ReplicaMutateError` repeated the same two variants beside its three real ones. They are now one `ReplicaArgError`, declared where the rule it enforces already lives, on the coroutine contract. The four verbs return it directly, because none of them can fail on its own terms: they read local state, ask for remote state and stage writes. `ReplicaMutate` composes it (`Arg(#[from] ReplicaArgError)`) beside `UnknownHandle`, `LinkExists` and `Ambiguous`, which are real failures of a mutation.

The verb left the message with the enums: "Replica OPEN failed" became "Replica coroutine failed". Nothing is lost, because a caller reaches this through `ReplicaClientError::Coroutine` from the verb it just called.

## A completed coroutine does not resume

`ReplicaSync` already refused; the other four handed back a default output. That is worse than useless: an empty report and an `Ok(())` are exactly what a run that genuinely did nothing returns, so a driver with a loop bug was told it had succeeded. All five now hold a terminal state, including `ReplicaUpgrade`'s second completion path, the nothing-to-upgrade one, which is the likeliest to be resumed by accident. One test per verb.

## One hub projection

`bound_placement`, `tombstone_placement` and `created_placement` each assigned the same twelve fields. `ReplicaHubItem::project` now carries the eight that are the item's shared content, and each caller settles only what the binding decides: the status, the base, the conflict revision, the ambiguous handles.

This one is **not** a line saving, and the audit's estimate of 35 lines was wrong: with its doc comment the projection costs about as much as the three literals it replaced. What it buys is the invariant. A field added to `ReplicaPlacement` was three edits, and a projection that forgot one produced a silently wrong answer rather than a compile error; it is one edit now.

## Also

The `Remove { to: None }` rename the audit listed is closed without a rename. The destination is optional because the consumer drops it when the move's other half already delivered, which only works while a delete and a relocation are one operation with an optional destination. What did not belong on the seam, the trash-routing policy, had already left the doc.

A `coroutine` capability was added to the spec: the two rules above are about the contract itself rather than about what any verb decides, and there was nowhere for them to live.

## Verification

- 203 tests green, `cargo clippy --all-targets` clean, `cargo fmt`, `cargo doc` without warnings.
- Four new tests, one per verb, for the resume-after-completion rule. The existing argument-error tests carried over to the shared type unchanged but for the name.

## Consumer impact

Breaking, on a 0.x line: four error types are gone and `ReplicaMutateError`'s two arg variants moved behind `Arg`. A consumer matching on them renames; one that only propagates them recompiles untouched.

Capabilities moved: `coroutine` (new), `hub`.

## Not in this change

`rekey` still drops every old handle before upserting the new spine, so a new handle space overlapping the old one puts a drop and an upsert of the same key in one batch and the outcome depends on the order the storage applies them. That is a correctness bug rather than compaction, and it stays in the audit backlog with the reproduction written down.
