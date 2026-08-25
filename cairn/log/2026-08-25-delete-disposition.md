---
cairn: log
date: 2026-08-25
change: delete-disposition
---

# What becomes of a refused delete is now a decision

The audit's sharpest small finding: a source can refuse a local delete two ways, and they meant opposite things. `push = false` reverted it, on the argument that a replica mirroring a source it does not own should let the member come back. `rights.remove = false` held the tombstone pending, on the argument that a later run might be allowed to push it. So `ReplicaPushRights::none()` was not `push = false`, and nothing in the crate said so.

Both arguments are good, which is why neither should be inferred from a switch that is about something else. `ReplicaSyncOptions::delete` now carries a `ReplicaDeletePolicy`, and every path that cannot take a delete goes through one `refuse_delete`: the read-only source, the forbidden remove, and the move whose staged edit cannot ride ahead of it. That last one had the same shape hiding in it, and it is the reason the check moved: a move must not go without its edit, or the relocated member carries the body the edit replaced, so it is a refused delete too.

`Revert` is the default. Between the two, holding is the one that fails silently: a tombstone the source refuses hides a member the source still holds, and an incremental enumeration never lists an untouched member again, so nothing ever brings it back. Reverting is visible, undoes an intent the user expressed, and converges. Holding is right when the refusal is a policy that may lift, an archive that takes appends but no deletes today, which is the consumer's knowledge and not the engine's.

## Verification

- 206 tests green, `cargo clippy --all-targets` clean, `cargo fmt`.
- `forbidding_remove_keeps_tombstone_undropped` became `forbidding_remove_reverts_the_tombstone_by_default`: it used to assert only that nothing was dropped, which is why the behaviour change did not fail it. It now asserts what the placement becomes.
- A new test runs both refusals under `Keep` and asserts the tombstone is untouched by either, which is the equivalence the audit asked for.

## Consumer impact

Behaviour change on a 0.x line: a consumer with `rights.remove = false` sees a refused delete reverted where it was held. `delete: ReplicaDeletePolicy::Keep` restores it exactly.

Capabilities moved: `sync`.
