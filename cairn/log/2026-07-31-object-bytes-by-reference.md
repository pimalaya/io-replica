---
cairn: log
change: object-bytes-by-reference
landed: 2026-07-31
---

# Object bytes by reference (bounded-memory body transfer)

Made object bytes deliverable by reference so a body never has to sit in memory
whole. `ReplicaFetchedItem.body` is now `Option<ReplicaFetchedBody>`, a two-variant
payload: `Inline { hash, bytes }` (the engine stores the bytes, as before) or
`Persisted { hash, size }` (an object the consumer already streamed into its blob
store during the fetch). `ReplicaWriteOp::StoreObject.body` became
`Option<Vec<u8>>` — `None` for a persisted reference, which the engine indexes
from `object`'s `(hash, size)` without writing bytes. The upgrade coroutine
derives the object metadata from the report (not `Vec::len`) and emits a byteless
`StoreObject` for the persisted case; the local-edit path in `mutate` keeps inline
bytes. `ReplicaObject` already carried `size`, so the merge, dedup and
`lookup_objects` short-circuit are untouched.

One unit test added (`a_persisted_body_stores_the_object_without_bytes`): a
`Persisted` fetch yields a byteless `StoreObject` whose object carries the
reported size, and the placement still pins the object at `Full`. All prior sync,
property, soft-delete and integration suites pass (the in-memory test fakes carry
`Inline` bodies, so their behaviour is unchanged); fmt clean.

Downstream (coordinated, same change id): io-pimdir gained streaming blob I/O and
a byteless-`StoreObject` path; neverest streams IMAP fetch/append end-to-end
(verified against Stalwart with a multi-MB message). cardamum-android adopted the
inline variant (compiles). himalaya-android-m3 got the body-shape edit but is
otherwise behind io-replica HEAD (uses removed `SetBase`, revision-less fetched
item) and needs a separate catch-up; its build was not verifiable in this
environment.

Spec updated: `storage` (ADDED: object write carries bytes or a reference; a Full
fetch may persist its own body).
