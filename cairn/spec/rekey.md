---
cairn: spec
capability: rekey
status: current
---

# Rekey

`ReplicaRekey` is the I/O-free coroutine that rebuilds one collection onto a new
handle space. A source may renumber every member without any of them changing (an
IMAP `UIDVALIDITY` bump, a provider migration, a mailbox restored from backup),
and every handle the replica holds is void at once. The spine is re-enumerated
and the stored state is carried onto it **by link id**, which is the only
identifier that survived, so bodies, summaries, bases and pending local edits are
kept rather than re-fetched.

It is a distinct verb rather than a resync because a resync cannot tell the two
apart: reading the new handles as unknown members and the old ones as vanished
would delete the collection and download it again, losing every staged edit on
the way.

### Requirement: A rebuild carries state over by link id
A rekey SHALL match each member of the new handle space to the placement holding
the same link id, and carry that placement's body, summary, level, base, flags
and pending local state onto the new handle. A member the old space does not
account for is an ordinary new placement; a placement the new space does not
account for is gone from the source.

Identity is the only thing a handle-space change leaves intact, so it is the only
thing the match may key on. Matching on anything derived from a handle would
match nothing, and matching on content would pair two copies of one body.

### Requirement: An ambiguity survives a rebuild
A placement carrying ambiguous handles SHALL keep them across a rekey.
Renumbering two copies of one identity does not merge them, and a rebuild that
cleared the record would resolve the freeze by forgetting why it was frozen,
which is the write [sync](sync.md) refuses to make.

### Requirement: A rebuild's drops say the row is superseded
Every drop a rebuild emits for a placement its own batch re-writes SHALL carry
`ReplicaDropReason::Superseded`, never `Deleted`. The item is not going anywhere:
the same batch upserts it under its new handle, and a storage sharing one item
across sources reads a `Deleted` drop as the item being gone and propagates a
removal to sources nobody touched.

The reason is also what licenses the rebind. A storage pins one handle per
binding and refuses to repoint it, because a repoint is how a second copy of one
identity used to be swallowed; a rebuild is the one case where the repoint is
correct, and the superseded handle is what tells the two apart. The licence is
per handle: a rebuild batch that also carries a genuine duplicate SHALL still
have that one frozen.

#### Scenario: A renumbered collection is not a duplicated one
- GIVEN a placement bound under a handle the rebuild supersedes
- WHEN the batch drops that handle and upserts the item under a new one
- THEN the binding follows the new handle, reads clean, and records no ambiguity

### Requirement: A rebuild is the only bump of the handle-space epoch
The consumer SHALL commit the rebuild's write batch and the collection's epoch
bump in one transaction (pimdir SPEC §12: `collections.generation`), so a reader
deriving an epoch-dependent protocol value from the store never sees a rebuilt
spine under the old epoch. Ordinary syncs, full resyncs from an expired
checkpoint and content changes SHALL NOT bump it.

### Requirement: A fetch refreshes the key at every tier
An upgrade SHALL adopt the key from the fetched item at both tiers, unlike the
link id, which is kept once resolved. The key is a projection of content rather
than an identity, so the later and better-informed derivation wins: a full body
carries the real date where an envelope may have carried none.

### Requirement: A key survives a rekey
Rebuilding a collection onto a new handle space SHALL carry each placement's key
over, preferring the one the rekey's meta fetch resolved and falling back to the
key the old placement held, so a handle-space change does not un-sort a
collection.

