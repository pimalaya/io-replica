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

### Requirement: Fetch batches are order-independent
A `WantsFetch` batch SHALL impose no ordering on its handles: the consumer MAY
fetch them in any order and concurrently, and SHALL return results keyed by
handle. The engine SHALL match fetched results by handle, not by position, so a
consumer servicing the batch across a connection pool is correct.

### Requirement: A linked body carries the base with it
A `Full` upgrade that resolves a placement's body from the object store instead
of fetching it SHALL record that body as the placement's base content, exactly
as the fetch path does. A placement holding a body its base does not is the
shape of a staged local edit, so a storage projects it dirty and the consumer
re-derives a change nobody made, on every sync, without ever converging.

### Requirement: A mutable body is fetched, never linked
A placement whose base carries a revision SHALL be fetched rather than linked
from the object store: its link id is left out of the lookup, and a hit on it is
ignored. A link id says two copies are the same item, not that they hold the
same bytes, and a source that rewrites bodies in place gives each copy its own
revision, so linking one copy's body under another's would record content no
fetch confirmed. Immutable content keeps the dedup, which is what it is for, and
the object store deduplicates the bytes of a fetched body regardless.

#### Scenario: The same message in two collections downloads once and reads clean
- GIVEN a based placement with no revision whose link id the object store already holds a body for
- WHEN it is upgraded to `Full`
- THEN it is written at `Full` with that body, its base holds the same body, and no fetch is requested

#### Scenario: A revision-carrying placement is fetched
- GIVEN a based placement carrying a revision whose link id the object store already holds a body for
- WHEN it is upgraded to `Full`
- THEN the body is fetched from the remote rather than linked from the store

### Requirement: A fetch establishes a link only for a not-yet-linked item
Applying a fetched item SHALL set the placement's `link_id` from the fetch **only
when the placement has none** — a `Meta` upgrade of a probed placement, or a
`Full` fetch of an item that never resolved a link. An already-linked placement
SHALL keep its link id when a later fetch (in particular a `Full` body fetch)
returns a different one, and simply rise to the fetched tier. A body fetch does
not re-identify an item; identity is resolved once, at the first fetch that
carries a link. This prevents a two-tier link disagreement (a server ENVELOPE
`Message-ID` the body parser misses, or a differently formatted fallback-digest
date) from stranding the linked item and duplicating it under the body's link.

#### Scenario: Largest-first hydration overlaps a heavy message
- GIVEN a consumer that fetches a Full batch across a bounded connection pool, largest first (by its own member sizes)
- WHEN the batch is hydrated
- THEN the heavy member is fetched concurrently with the light ones, results are matched by handle, and no fetch order is assumed

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

### Requirement: An upgrade revisits what it never got
An upgrade SHALL revisit a placement whose level claims a tier it does not hold: `Full` with no object, `Meta` with no summary. The level is a claim and the payload is the fact, and nothing else revisits what already reads as reached, so such a row would be skipped for good.

#### Scenario: A body-less full row
- GIVEN a placement recorded at `Full` holding no object
- WHEN it is upgraded to `Full`
- THEN it is fetched rather than skipped

### Requirement: A push is counted when it matched
`ReplicaSyncReport::pushed` SHALL count the changes this run derived and the remote accepted, not the results the consumer reported: a result naming a handle nobody pushed, or naming one twice, cannot inflate it.
