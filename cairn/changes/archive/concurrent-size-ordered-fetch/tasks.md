---
cairn: tasks
change: concurrent-size-ordered-fetch
---

# Tasks

## io-replica (this repo)

- [ ] Add an optional octet `size` to `ReplicaRemoteItem`, populated by enumerate
      where the backend reports it cheaply (IMAP `RFC822.SIZE`, DAV
      `getcontentlength`); `None` otherwise. Advisory scheduling data only.
- [ ] Guarantee (and test) that a `WantsFetch` batch is order-independent:
      results are matched by handle, not position, so a pooled/reordered fetch is
      correct.
- [ ] Fold spec: `sync` (two ADDED requirements: member size; fetch order-
      independence).
- [ ] Log entry.

## Coordinated downstream Cairn change

- [ ] **neverest** `concurrent-size-ordered-fetch`: bounded worker pool (one
      connection per worker, size capped to the server's connection limit)
      servicing the Full-fetch batch largest-first; whole-message jobs streaming
      per `object-bytes-by-reference`; index writes serialise on the single-writer
      store while bodies stream lock-free.
