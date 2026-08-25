---
cairn: change
id: engine-compaction
status: landed
created: 2026-08-25
---

# What the audit found repeated rather than wrong

## Why

`engine-algorithm-audit` landed its correctness half and left a compaction list, of which the string-newtype macro, `staged_edit`, `may_push`, the push counter and `ReplicaCollection` have since gone. Three items are left, and none of them is a bug: they are the same thing written more than once, and the cost is that a reader has to check whether the copies agree.

- **Four argument-error enums.** `ReplicaOpenError`, `ReplicaUpgradeError`, `ReplicaRekeyError` and `ReplicaSyncError` are byte-identical but for the verb name in the message, and `ReplicaMutateError` repeats the same two variants beside its three real ones. They all say one thing: the driver broke the coroutine protocol. Four types say it because each verb grew its own.
- **Three near-identical hub projections.** `bound_placement`, `tombstone_placement` and `created_placement` each assign the same twelve fields, eight of which are the item's shared content in every case. A field added to `ReplicaPlacement` has to be threaded into three places, and a projection that forgets one is a silent wrong answer rather than a compile error.
- **Only `ReplicaSync` refuses to resume when it is done.** The other four hand back a default output, which a caller cannot tell from a run that genuinely did nothing. The audit listed this under compaction; it is really a small correctness rule the sync already states and the rest do not.

## What

- One `ReplicaArgError` on the coroutine contract, where the rule it enforces is already written. The four verbs with no failures of their own return it directly; `ReplicaMutate` composes it beside its three real variants.
- One projection builder on `ReplicaHubItem`: the shared content stated once, the caller settling what the binding decides.
- A terminal state in every coroutine, so a resume after completion is `UnexpectedArg` everywhere rather than an empty success in four places out of five.

## Scope / non-goals

- No behaviour change except the last one, which turns four silent empty successes into errors.
- The `Remove { to: None }` rename the audit also listed is closed without a rename: the destination is optional because the consumer drops it when the move's other half already delivered, and that only works while a delete and a relocation are one operation. What did not belong on the seam, the trash-routing policy, is already out of the doc.
- `rekey`'s remaining order dependence (a drop and an upsert of one handle in a batch when the new handle space overlaps the old) is a correctness bug, not compaction, and stays in the audit backlog.
