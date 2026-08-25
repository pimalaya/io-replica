---
cairn: delta
change: chunked-pushes
---

## ADDED Requirements

### Requirement: A run records its pushes in bounded chunks
A sync SHALL push its derived changes in bounded chunks, yielding the writes a chunk produced before the next chunk is pushed: one `WantsPush` per chunk, each followed by the `WantsWrite` recording its outcomes. The bound is the engine's (`ReplicaSync::PUSH_CHUNK`), not a consumer option, because what it bounds is a crash window rather than throughput.

Pushes stay at-least-once, but the window is one chunk rather than one run: a crash between a serviced push and the write recording it replays only the chunk whose write never landed, since every earlier chunk is already recorded and a later one was never sent. A driver therefore MUST NOT assume one push and one write per run; the reference driver services yields in a loop and needs no change.

Only the handles of the chunk being serviced SHALL be resolved when its outcomes land: a handle a chunk never reported on is left pending exactly as before, and a later chunk's handles are still waiting for their own outcome.

#### Scenario: A crash after the first chunk
- GIVEN a run deriving more changes than one chunk holds
- WHEN the first chunk is pushed and recorded, and the write recording the second chunk is lost
- THEN the placements of the first chunk are recorded clean, and only the second chunk's are still pending

### Requirement: The checkpoint lands in the last write
The checkpoint the enumerate reported SHALL land in the write that follows the final chunk, and SHALL stay the pre-push one, which is what makes the engine's own echo re-listed by the next delta enumeration. An intermediate chunk's write SHALL NOT carry it, so an interrupted run resumes from the same cursor rather than from one claiming its unrecorded pushes were seen.

### Requirement: Every change carries an idempotency key
A `ReplicaChange` SHALL be a `ReplicaChangeKind` (what the remote is asked to do, the four verbs that were the change itself) plus the `ReplicaChangeKey` naming it. The key SHALL be derived from the collection, the handle, the kind and the target state the change makes true: the flag set of a `SetFlags`, the body of an `Update`, the destination of a `Remove`, and the identity, markers, origin and body of an `Add`. The same derived change SHALL key the same on every run, and changes differing in any of those SHALL key differently.

A precondition is deliberately not part of it: `if_match` states what the change was attempted against, not what it makes true, and a retry of one operation is one operation.

The split is what keeps the key honest. `ReplicaChange::new` is the only way to make one, so a change cannot exist without a key; the engine derives a *kind* and keying it is the last thing that happens to it, so there is no state in which a keyed change is half-built or names something other than what it carries.

Recording the key is what makes the at-least-once contract actionable for every kind: an add could already be recognised by its `link_id`, but a flag set, a content update and a remove carried nothing a consumer could log to recognise a replay of *this* operation.

#### Scenario: A replayed change keys the same
- GIVEN a change an interrupted run pushed
- WHEN the next run derives it again from the same local state
- THEN it carries the same key, and the consumer recognises the replay
