---
cairn: change
id: chunked-pushes
status: landed
created: 2026-08-25
---

# A crash replays the tail, not the run

## Why

A sync is one `WantsWrite`. Every pull, rebase, drop, rekey and the checkpoint land in a single batch after every push has been serviced, so a crash between the pushes and that write replays **every** push on the next run, not the ones that had not been recorded.

The crate is honest that pushes are at-least-once and puts the burden on the consumer: treat a remove of an already-missing member as accepted, use an add's `link_id` to detect that it already landed. But only adds carry a key, and no change carries anything a consumer could log to recognise *this* operation on replay. A flag set re-applies harmlessly; a content update re-applies over a body the remote may have changed since; a keep-both duplicate carries no link id at all.

The property suite covers crash-after-push and asserts no loss, but only under the default `Manual` conflict policy and with a "spurious conflict is acceptable" escape hatch. That is the shape of the gap: the engine converges, at the cost of work nobody asked it to redo.

## What

- **Chunk the push-and-record cycle.** Instead of pushing everything then writing everything, push a bounded number of changes, write the state they produced, repeat. The crash window becomes one chunk rather than one run, and a resumed sync replays at most that chunk.
- **A deterministic idempotency key on every derived change**, not only adds: a hash over the collection, the handle, the change kind and the target state. A consumer that records the keys it has applied recognises a replay of any kind, which is what makes the at-least-once contract actionable rather than advisory.
- The checkpoint keeps landing last, and keeps being the pre-push one: that is what makes the engine's own echo re-listed on the next delta, and it is unaffected by chunking.

## Scope / non-goals

- The chunk size is the engine's, not a consumer option: the point is bounding a crash window, not tuning throughput.
- No change to what is derived, only to when it is recorded.
- **Breaking**: `ReplicaChange` gains a field and the coroutine yields several push/write pairs per run, so a driver that assumed one of each has to loop. `ReplicaClient` absorbs that for consumers using it.
