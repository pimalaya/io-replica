---
cairn: change
id: generated-coverage-of-conflicts-and-the-hub
status: landed
created: 2026-08-29
---

# The generators reach the states the properties are named after

## Why

Measured over 20000 generated cases each, `mutable_interleavings_converge_after_resolution` marked a `ReplicaStatus::Conflict` in 3.9% of them, resolved one in 2.7%, landed a `conflict_object` in 0.17%, and staged a content-versus-content collision in 10.8%. At the default 256 cases that is nine conflicts and zero diverging bodies. The rates are flat in the case count, so raising `PROPTEST_CASES` buys samples of the same distribution and never a different one.

Four causes, each measured rather than guessed:

- `arb_mut_op` is a flat `prop_oneof!` over thirteen variants, and a conflict needs a local and a remote edit on the same handle with no sync between them;
- the model resolves conflicts blind, never calling the upgrade a real consumer calls, which is the only thing that ever writes a `conflict_object`;
- every model seeds two members, so a delete and a move empty the live set and a fifth of all generated ops find nothing to act on;
- `a_crashed_write_never_loses_data` draws its crash point over write batches a sequence rarely issues, so in 54% of cases the crash fired in the terminal drain, after the last user intent.

The hub axis measured exactly zero at every case count, structurally: tests/property.rs contains no hub, and src/hub.rs, the second largest module in the crate, is covered by roughly a dozen hand-scripted scenarios. Yesterday's data-loss fix there (`ReplicaSourceBinding::shared_object`) has no generated coverage at all.

## What

Weight the mutation generator toward the edits, upgrade the conflicted handles in the model's resolver the way a consumer does, seed five members instead of two in all four models, and narrow the crash budget to the range that lands inside the op sequence.

Add a model-based property test over the hub: three sources bound to shared items, generated edits, flag changes, deletes, locally-authored creates, arrivals, server-side deletes and syncs, asserting at quiescence that every source converges on one body, that no source is ever read as diverging from itself, that a genuine divergence between two sources is reported rather than silently resolved, and that every staged body reached the shared item, is held as a reported conflict, or was superseded by a strictly later action on the same item.

The hand-written hub fixture moves to tests/common so both targets drive the same store.
