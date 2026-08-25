---
cairn: spec
capability: sync
status: current
---

# Sync

`sync` reconciles one collection's local replica against one remote through a
three-way merge of Local, Base and Remote per placement, keyed on the handle.
Flags merge element-wise and never conflict; only divergent mutable content is
kept as a conflict. It is tuned by `ReplicaSyncOptions`.

It reconciles what the replica holds and never fetches a payload: raising a
placement up the detail ladder is [upgrade](upgrade.md)'s, and rebuilding a
collection onto a new handle space is [rekey](rekey.md)'s.

### Requirement: Three-way reconcile
The engine SHALL merge each candidate placement over `(local, base, remote)`,
pushing local-won changes and pulling remote-won changes, comparing per-placement
identities (the flag set and, for mutable-content backends, a content revision)
rather than raw bytes.

### Requirement: Push-outcome discipline
The engine SHALL confirm a push with the remote before rewriting local state: a
flag, content, delete or create push is stashed and applied only on an
`Accepted` outcome; a `Rejected` (or unreported) outcome leaves the placement
dirty, tombstoned or provisional so the next sync retries it.

### Requirement: Push direction
`ReplicaSyncOptions.push` SHALL be the master push switch. When false the source
is treated read-only: local flag and content changes are kept dirty and never
pushed, remote-won changes are still pulled, and a local delete is applied to the
replica only. When true, the `ReplicaPushRights` refinement SHALL gate each push
kind independently.

#### Scenario: Read-only source keeps local edits
- GIVEN a placement with a locally-changed flag set and `push = false`
- WHEN the collection is synced
- THEN the engine emits no push and the placement stays dirty

### Requirement: Granular push rights
`ReplicaSyncOptions` SHALL carry a `ReplicaPushRights` refinement with an
independent boolean for each push kind (`flags`, `content`, `add`, `remove`),
defaulting to all-permitted. When `push` is true, the engine SHALL derive a push
of a given kind only when the matching right is permitted; a forbidden push kind
is treated like the read-only path for that kind alone (the local change is kept
pending and never pushed, and a forbidden delete is not applied to the replica),
while other kinds still propagate.

#### Scenario: Flags allowed, deletes forbidden
- GIVEN a source with `push = true`, `rights.flags = true`, `rights.remove = false`
- AND a placement with a locally-changed flag set and another locally tombstoned
- WHEN the collection is synced
- THEN the flag change is pushed
- AND no remove is pushed, and the tombstone is retained (not dropped) for a later sync

### Requirement: Per-item delta events
The sync SHALL emit a `ReplicaEvent` for each per-item outcome it produces — a
member added, its flags changed, its content changed, it vanished, it
conflicted, or a create the remote accepted — in order, and carry them on
`ReplicaSyncReport.events`. Events are spine-level data (a handle, no body), so
emitting them enters no I/O. Hooks and richer reporting ride the events; the
report's counters summarise them.

#### Scenario: A remote add emits Added
- GIVEN a remote that lists a member absent locally
- WHEN the collection is synced
- THEN the report carries a single `Added` event for that handle

#### Scenario: An accepted create is reported under its assigned handle
- GIVEN a locally-created member the remote accepts and assigns a handle
- WHEN the create is confirmed
- THEN the report carries a `Created` event for the server-assigned handle

### Requirement: Headless conflict resolution
`ReplicaSyncOptions` SHALL carry a `ReplicaConflictPolicy` (`Manual`,
`PreferLocal`, `PreferRemote`, `KeepBoth`; default `Manual`) applied when content
diverges on both sides of a based placement. `Manual` marks the placement
conflicted and waits for the consumer's edit. `PreferRemote` drops the local
edit and pulls the remote. `PreferLocal` pushes the local body as an `Update`
gated on the *observed* remote revision (overwriting the current remote), and
falls back to `Manual` when the source may not push content. `KeepBoth` pulls the
remote into the placement and stages the local body as a fresh `Created` member
so neither version is lost. A base-less create-collision is always kept as a
conflict regardless of the policy. Immutable-content backends report no revision
and so never reach a content conflict.

#### Scenario: PreferRemote discards the local edit
- GIVEN a based placement edited locally and changed on the remote, `conflict = PreferRemote`
- WHEN the collection is synced
- THEN the remote content is pulled and no conflict is recorded

#### Scenario: KeepBoth preserves both versions
- GIVEN a based placement edited locally and changed on the remote, `conflict = KeepBoth`
- WHEN the collection is synced
- THEN the remote is pulled into the placement
- AND the local body is staged as a new `Created` member for the next sync to append

### Requirement: A move delivers exactly one copy
A move is staged as a create in the target plus a remove of the source, each derived by its own collection's sync in whichever order the consumer runs them, and both halves can deliver the item on their own: the create by copying from its origin, the remove by relocating the member into its destination. `ReplicaChange::Remove` carries the `link_id` its destination would receive, and a consumer SHALL relocate only while that destination does not already hold it; otherwise the create already delivered, and the remove is a plain delete of the source.

Neither half may be dropped in favour of the other: the remove is what keeps a move safe when the target syncs last, since the source is relocated rather than deleted out from under a copy that never ran, and the create is what keeps a move working through a hub, whose bindings carry no origin. When the target syncs last its create finds its origin already relocated, so the push is rejected and the placeholder stays visibly pending: an add carries no key that separates a second copy the user asked for from one the remove already served.

An item whose link id is not resolved yet has no such key, so `ReplicaMutation::Move` stages the source half alone for it: the relocation delivers it, and the target picks it up on its next enumerate.

#### Scenario: The target syncs first
- GIVEN a linked member moved into a target
- WHEN the target's sync copies it, then the source's sync derives its remove
- THEN the destination already holds the link id, so the source is deleted rather than relocated, and the target holds exactly one member

#### Scenario: The source syncs first
- GIVEN the same move
- WHEN the source's sync runs first
- THEN the member is relocated into the target, and the target's create finds its origin gone: it is rejected and stays visibly pending rather than delivering a second copy

#### Scenario: A never-fetched item
- GIVEN a member whose link id is not resolved
- WHEN it is moved
- THEN only the source tombstone is staged, and the target holds exactly one member in either sync order

### Requirement: A read-only source reverts a local delete
Where `ReplicaSyncOptions::push` is false a local delete can never propagate and the replica mirrors the source, so the merge SHALL revert the tombstone rather than apply it. Applying it and waiting for a later enumerate to re-add the member only works against a complete snapshot: an incremental enumerate never lists an untouched member again, so the dropped row would never come back, leaving the replica permanently short of an item the source still holds. Reverting also keeps whatever the placement had cached.

#### Scenario: A delta enumerate
- GIVEN a read-only source with a locally deleted member, unchanged upstream
- WHEN an incremental enumerate lists nothing
- THEN the placement is written back clean, keeping its body

### Requirement: Both axes reconcile, every run
The flag axis SHALL run for every placement present on both sides, including one whose content axis derived a push. A push result is matched by handle, so one handle yields at most one change: the flag axis withholds its own push in that case, but still merges and writes. Skipping it outright loses a remote flag change until some later run happens to list the item again, which an incremental enumerate may never do.

#### Scenario: A remote flag change beside a local content edit
- GIVEN a placement with a staged content edit whose remote also changed a flag
- WHEN the sync derives the content push
- THEN the merged flags are written in the same batch

### Requirement: A keep-both duplicate is a new item
The duplicate a `KeepBoth` resolution stages SHALL carry an identity derived from the body it forked, both as its provisional handle and as its link id. It is a new item rather than another copy of the one it forked from, since the two hold different bodies, and giving it the original's link id would have a storage sharing items by link collapse the fork back. Deriving both from the body is also what makes two resolutions staged before either is pushed keep both versions, and what gives the retried add an idempotency key.

#### Scenario: Two resolutions of one handle
- GIVEN two keep-both resolutions of the same placement, forking different bodies
- WHEN both are staged before either is pushed
- THEN their handles and link ids differ, so neither overwrites the other

### Requirement: A push is counted when it matched
`ReplicaSyncReport::pushed` SHALL count the changes this run derived and the remote accepted, not the results the consumer reported: a result naming a handle nobody pushed, or naming one twice, cannot inflate it.

### Requirement: An ambiguous identity derives nothing
A placement carrying ambiguous handles SHALL read as `ReplicaStatus::Ambiguous`, and the engine SHALL derive no change for it in either direction while it does: no push of any kind on any axis, no vanish-delete, no staged mutation, and no cross-source propagation.

Its absence from a complete snapshot in particular SHALL NOT be read as the item being gone from that source: the source demonstrably holds another copy of the identity, and deleting on that reading is what removes the only copy on a source nobody touched.

The rule is *derive nothing* rather than *pick a copy* because the engine has no basis for choosing which copy a change belongs to, and choosing wrongly destroys mail. A frozen item is mirrored zero times rather than once, which is the cost of not guessing.

**The freeze is one item wide, and SHALL NOT halt anything.** A run meeting one SHALL reconcile the rest of the collection normally, in both directions, and SHALL complete; a staged mutation against a frozen placement SHALL be refused as that mutation rather than as the run, so a queued one parks and the drain carries on. The engine reports the ambiguity and skips the item, and what to do about it is the user's: which copy to keep is a judgement the engine does not have and a schedule it does not control.

Scoping is what makes deriving nothing an acceptable answer at all. The same rule applied a batch or a collection wide would strand a mailbox on one double delivery, which is a worse outcome than the mispairing the freeze exists to prevent, and it would be one the user could not clear without the engine's help.

#### Scenario: A frozen item does not stop its neighbours
- GIVEN a collection holding one frozen identity beside ordinary members
- WHEN it is synced with remote and local changes on both
- THEN the ordinary members pull and push as usual, and the frozen one derives nothing

#### Scenario: An ambiguous placement is never deleted by a vanish
- GIVEN an ambiguous placement bound to a source
- WHEN a complete snapshot of that source omits its handle
- THEN no delete is derived, on that source or on any other holding the identity

### Requirement: An ambiguity clears when the source resolves it
A complete snapshot that omits a recorded ambiguous handle, or a delta that reports it vanished, SHALL drop that handle: the source is saying that copy is gone. A delta that merely does not mention it says nothing and clears nothing. A placement left with no ambiguous handles lands `Clean` and reconciles in that same run, so what it slept through is picked up rather than waiting for an enumeration that may never list it again.

A rekey SHALL carry the state over a handle-space change: renumbering the copies does not merge them.

#### Scenario: Resolving the duplicate resumes the sync
- GIVEN an ambiguous placement
- WHEN an enumeration reports the identity under a single handle
- THEN the ambiguous handles are cleared and the placement reconciles normally


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

### Requirement: An enumeration is ordered by handle
`ReplicaRemoteSnapshot::items` SHALL be sorted by handle and SHALL list each handle at most once. The merge walks it beside the local placements in that order rather than indexing it, which is what keeps a whole-collection sync from copying both key spaces to join them; protocols hand it over sorted already, an IMAP SEARCH returning ascending UIDs.

The engine SHALL NOT depend on a consumer honouring this: a snapshot that arrives unsorted is sorted, and a handle listed twice is collapsed to its first item, so getting it wrong costs a pass rather than correctness.

#### Scenario: An unordered enumeration
- GIVEN a snapshot whose items arrive in any order
- WHEN the collection is synced
- THEN it derives exactly what the same snapshot sorted derives

### Requirement: The merge joins the two sides rather than copying them
The merge SHALL pair local placements with remote items by walking both in handle order, taking each placement rather than copying it per candidate. A copy SHALL be made only where a write takes ownership of one.

This is a shape requirement rather than a behaviour one, and it is stated because the shape is what the cost is: the candidate set, the order it is merged in and every change derived from it stay exactly as they were, and the property comparing a delta run against a full one is the guard on that.

A delta snapshot keeps its own rule for which of the joined handles is a candidate: the ones it reported changed or vanished, plus every locally non-clean handle, whose pending push it would otherwise never revisit. A never-based one (a staged create) is a candidate with no remote state to merge against, since the enumeration will not mention it until it lands.

### Requirement: A write batch is bounded and cut between candidates
The merge SHALL hand a write batch over once it holds `ReplicaSync::WRITE_CHUNK` writes, rather than holding one write per member until the last candidate is resolved. What this bounds is memory rather than a crash window: a lost batch costs a re-merge, where a lost push costs a round trip.

A batch SHALL be cut between candidates and never inside one. The writes one candidate derives are consistent only together: a keep-both resolution stages the local body as a new member beside the pulled placement it forked from, and a cut between them would lose that body if the next batch never landed.

The checkpoint rule is unchanged and is what makes a partially merged run safe to resume: an intermediate batch carries no checkpoint, so a run interrupted mid-merge re-enumerates from the same cursor and re-derives whatever it had not recorded.

#### Scenario: A merge larger than one batch
- GIVEN a snapshot deriving more writes than one batch holds
- WHEN the collection is synced
- THEN the writes arrive in several batches, and only the last one carries the checkpoint

### Requirement: A refused delete follows one policy
`ReplicaSyncOptions` SHALL carry a `ReplicaDeletePolicy`, consulted wherever a local delete cannot go: `push` is false, `rights.remove` is false, or a move's staged edit cannot ride ahead of it (the move must not go without the edit, or the relocated member would carry the body the edit replaced).

`Revert` SHALL undo the delete, landing the placement `Clean` with whatever it had cached. `Keep` SHALL hold the tombstone as it is, so a later run that may push derives the remove again. Either way the engine SHALL derive no push and SHALL NOT apply the delete to the replica.

`Revert` is the default. A held tombstone hides a member the source still holds, and hides it for good: an incremental enumeration never lists an untouched member again, so nothing brings it back. Holding is right only when the refusal is a policy that may lift, which is the consumer's knowledge, not the engine's.

Deletion is the only axis needing this. A refused flag or content change stays dirty and re-derives every run, but a refused delete has to be either undone or held.

A source bound to a hub SHALL be given `Keep`. Reverting states that this source still holds the member, which the hub reads as the item being alive (add-beats-delete across sources): the deletion is cleared for every source and the item is mirrored back to the one it was deleted on. Both readings are coherent, and only the consumer knows which it means.

#### Scenario: The two refusals agree
- GIVEN a tombstoned placement the source still holds
- WHEN it is synced with `push = false`, and again with `rights.remove = false`
- THEN both follow the same policy: reverted by default, held under `Keep`
