---
cairn: log
date: 2026-08-25
change: chunked-pushes
---

# A crash replays the tail, not the run

A sync used to be one `WantsPush` and one `WantsWrite`: every pull, rebase, drop, rekey and the checkpoint landed in a single batch after the last push had been serviced, so a crash in that window replayed *every* push the run derived. It now alternates, `ReplicaSync::PUSH_CHUNK` changes at a time: push a chunk, write what it produced, push the next. The window is one chunk, an earlier chunk is already recorded, and a later one was never sent at all.

Two things the implementation forced. The pending maps could no longer be cleared wholesale when outcomes land, since a later chunk's entries are still waiting for their own push: only the handles of the chunk being serviced are resolved, which keeps the existing "a handle the push never reported on stays pending" rule intact per chunk. And the checkpoint had to come out of `reconcile`, which appended it to the batch as it finished merging; it is held on the coroutine and appended to the batch that closes the run, so an interrupted run resumes from the same cursor rather than from one claiming its unrecorded pushes were seen. It is still the pre-push checkpoint, which is what makes the engine's own echo re-listed by the next delta.

The chunk size is the engine's, a `pub const` rather than a `ReplicaSyncOptions` field: what it bounds is a crash window, not throughput. It is public because a consumer sizing its write transaction wants to know the bound, not because it may pick one.

## A change is a kind plus its key

`ReplicaChange` was an enum of four verbs. It is now a struct: the `ReplicaChangeKind` those four verbs became, plus the `ReplicaChangeKey` naming it, derived from the collection, the handle, the kind and the target state. That is what makes the at-least-once contract actionable for every kind: an add could already be recognised on replay by its `link_id`, but a flag set, a content update and a remove carried nothing a consumer could log to recognise a replay of *this* operation.

The split is the point, and it is why the key is not simply a field on each of the four variants. A field per variant is a second copy of what the variant already says, and something has to fill it: either seven construction sites each restating the key's inputs, or a placeholder stamped afterwards, which is a value that is briefly wrong. With the kind as its own type there is no such state. The merge derives a kind, `ReplicaChange::new` keys it in one place, and a `ReplicaChange` cannot be built any other way, so it never exists unkeyed and never names something other than what it carries. `handle()` moved onto the struct along the way, replacing the four-arm match the tests and the driver were each writing.

The digest is FNV-1a over `\0`-terminated fields, sixteen hex characters, written here rather than pulled in: the crate depends on `log` and `thiserror` and nothing else, and an idempotency key needs determinism, not resistance to a forged collision. `if_match` is deliberately excluded: a precondition states what the change was attempted against, not what it makes true, and a retry of one operation is one operation.

## Verification

- 195 tests green, `cargo clippy --all-targets` clean, `cargo fmt`.
- `tests/chunked_pushes.rs` drives 67 pending flag pushes through `ReplicaClient`: the run splits them 64 then 3, one write batch each, and dropping the second write leaves the first 64 recorded clean while only the remaining 3 stay dirty. Both tests fail against an unchunked engine.
- A unit test pins the yield sequence itself (push, write, push, write), that no second-chunk row rides the first batch, and that the checkpoint is in the last write and only there.
- `change.rs` covers the key: the same derived change keys the same, the collection, handle, kind and every target-state field key apart, an unknown flag set does not key as an empty one, and a precondition does not enter the key.
- The existing crash property (`a_crashed_write_never_loses_data`) still holds unchanged. Its "a spurious conflict is acceptable" allowance is untouched: its scenarios derive far fewer than one chunk of changes, so a run is still a single chunk there and the allowance still describes what it covers.

Capabilities moved: `sync`.

## Handed over

io-pimdir and neverest see several push/write pairs per run where they saw one, which the reference driver absorbs. What they have to change is the shape they match on: a push loop reads `change.kind` where it read the change itself. What they gain is `change.key`, to record per applied change and look up on replay.
