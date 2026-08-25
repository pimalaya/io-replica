---
cairn: change
id: duplicate-link-id-freeze
status: landed
created: 2026-08-25
---

# An identity a collection holds twice is frozen, not guessed

> Cross-repo change, same id in three repos, in this order:
> **io-replica** (here: the invariant, the detection, the rules) →
> **io-pimdir** (persist it, stop the silent repoint) →
> **neverest** (report it, end-to-end test). Each proposal restates enough
> context to be read alone.

## Why

One collection may hold two items with the same link id: two messages with one
`Message-ID` (double delivery, a retried `APPEND`, a restore, a migration from
another provider). The engine's model has nowhere to put the second: a placement
is identified by `(collection, link_id)` and a source binds it with **one**
handle. Multiplicity is first class *across* collections and absent *within*
one, so the sync treats the bound handle as the identity and everything the
protocol says about the other copy is invisible to it.

Reproduced against two IMAP servers (`neverest/tests/stalwart2.sh`, A on :143
and B on :144), one copy of a message on A and two on B, synced two-sided:

1. The first sync pairs A's copy with **one** of B's; the other is unbound and
   invisible.
2. Deleting the bound copy on B propagates a delete to A and **removes the only
   copy there**, while B still holds the message. Remote data loss on a side the
   user never touched.
3. Nothing more happens while B keeps its QRESYNC checkpoint, the untracked twin
   never appearing in a delta. Lose that checkpoint (a UIDVALIDITY bump, a
   server without QRESYNC, a reset) and the full enumeration revives the
   retained row and **re-appends the message to A**, days after the user deleted
   it.

The engine did nothing wrong given what it was told: a source reported that a
handle vanished, and a vanished bound handle means the item is gone from that
source. What is wrong is that the claim "this handle is the identity" was
accepted for an identity the collection holds twice, and that the second copy
was then silently overwritten one layer down.

**Why the engine and not the connector.** Three findings put it here:

- The evidence is destroyed at *write* time, not at delete time. io-pimdir's
  binding update sets `handle = :handle` on the existing
  `(collection, link_id, source)`, so the second copy silently repoints the
  binding and no layer can afterwards tell that the source holds two. Nothing
  reactive at the vanish can work, because by then the fact is gone.
- The freeze has to be **sticky**. The twin appears in exactly one enumeration,
  the one that discovers it; with QRESYNC it is never mentioned again (step 3
  above is the proof). A per-run check in a connector therefore freezes once and
  forgets, leaving the item live and deletable on the next run. Sticky means
  persisted, and persistence belongs to the store, whose rules are this crate's.
- The cost of putting it here is small and paid once: `ReplicaStorage` has one
  implementation outside this crate's tests, io-pimdir, and every consumer
  (neverest, himalaya-android-m3, android, linux, pimgate) reaches the engine
  through it.

Leaving it to consumers means every one of them re-derives the same check
against a store that keeps destroying the evidence underneath.

## What

**Refuse to resolve an ambiguous identity, and refuse to act on it**, on the
same terms the crate already applies to a content conflict: mark it, leave it
alone, let the consumer report it, and keep it marked until the source holds the
identity once.

- **Detect where identity is assigned.** `ReplicaUpgrade` applies a fetched link
  id and already carries the rule *"a fetch establishes a link only for a
  not-yet-linked item"*. Its sibling lands beside it: a fetch SHALL NOT
  establish a link another placement of the same collection already holds. The
  coroutine holds the collection's placements at that moment, so the check is
  local and free.
- **Carry the ambiguity as state, not as an inference.** The losing handle is
  recorded on the binding that holds the identity, so the fact survives the
  round trip through the storage and the next enumeration that never mentions
  the twin again.
- **Project it as a status the rules refuse to act on.** A placement whose
  binding carries other handles projects `ReplicaStatus::Ambiguous`: the sync
  derives no push and no vanish-delete for it, the hub neither pairs it across
  sources nor propagates a delete of it, `mutate` refuses to stage against it,
  and `rekey` carries it over untouched.
- **Clear it when the source resolves it.** An enumeration that reports the
  identity once again clears the extra handles, and the item resumes syncing
  with no further ceremony.

### Shape (the one open decision)

Proposed: `ReplicaSourceBinding` gains `ambiguous_handles: Vec<ReplicaHandle>`,
the handles this source holds for this identity beside the bound one, mirroring
`conflicted` / `conflict_revision` exactly (state on the binding, because "this
source holds it twice" is a per-source fact, not a shared one), and
`ReplicaStatus` gains `Ambiguous`.

A status **variant** rather than a loose boolean is deliberate: the enum is
matched in `sync`, `hub`, `mutate` and `rekey`, so the compiler forces every
rule to say what it does with an identity the engine cannot resolve, which is
exactly the omission that produced the data loss. The alternative shape, a flag
on the item rather than the binding, is worth a moment's review: it is simpler
to persist and wrong for a two-sided store, where one source may hold the
duplicate and the other not.

**Breaking**, therefore 0.5.0: a new public field and a new status variant.

## Scope / non-goals

- **A frozen item is mirrored zero times, not once.** For propagation that is
  strictly better, nothing being destroyed or resurrected. For a backup it is a
  regression, a backup that skips messages being the failure it exists to
  prevent. Only 1:N bindings serve that case, and this change does not pretend
  to.
- **Bindings stay 1:1.** Letting a binding hold a *set* of handles per source,
  an item deleted on a source only when every one of them vanishes, is the
  faithful fix, a larger model and schema change, and the natural successor to
  this one. Deliberately not proposed here.
- **A per-copy link id is not an option.** Folding the handle into the link id
  makes it server-local, so a copy on one side has no counterpart on the other,
  both sides append to each other, and a duplicate becomes an unbounded
  duplication loop. A link id stays content-derived, and this crate never
  derives one.
- **The engine does not repair a duplicated collection**, and does not judge
  one: RFC 5322 §3.6.4 binds the *generator* of a `Message-ID` and says nothing
  about what a store may hold, so two copies of one message is redundancy, not
  corruption.
