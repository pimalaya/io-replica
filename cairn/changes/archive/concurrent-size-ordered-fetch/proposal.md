---
cairn: change
id: concurrent-size-ordered-fetch
status: landed
created: 2026-07-31
---

# Concurrent, size-ordered hydration

## Why

Hydration is sequential over one connection per side, so one heavy message can
stall the tail of a sync — worst of all when it is scheduled last, with nothing
behind it to overlap. This is a throughput / head-of-line-blocking problem, an
**optimisation, not a correctness fix**.

It **depends on** [`object-bytes-by-reference`]: running N fetches at once
without streaming would multiply the memory problem (N × full body in RAM).
Streaming must land first; then concurrency is safe (total memory ≈
pool_size × chunk buffer).

The right unit of concurrency is **one whole message on its own connection**, not
a chunk. A single message is one ordered, length-prefixed literal on one socket
(FETCH) and one on the target (APPEND) — it cannot be split across workers, and
per-chunk tasking is pure scheduling overhead for zero parallelism. Parallelism
comes from **multiple connections**, each running a whole-message job.

## What

- **io-replica**: make explicit that a `WantsFetch` batch is order-independent
  and MAY be serviced concurrently, results keyed by handle. (A spine `size`
  field on `ReplicaRemoteItem` was considered but dropped during implementation:
  the engine never schedules, and the scheduling consumer already knows sizes
  from its own enumerate — so size stays a consumer concern, not a spine field.)
- **neverest** (its own Cairn change): a small bounded pool of connection-owning
  workers services the Full-fetch batch, largest-first — sizes read from
  neverest's own envelope listing (IMAP `RFC822.SIZE`) — streaming each body per
  `object-bytes-by-reference`. Body bytes stream lock-free; only the small index
  commit serialises on the single-writer store.

## Scope / non-goals

- **Depends on `object-bytes-by-reference`** — do not start until it lands.
- **Not real-time / watch.** neverest stays sync-on-demand; this only overlaps
  work within one run.
- **No chunk-level tasking** (unparallelisable, ordering-bound — see Why).
- **No async reactor.** A blocking pool of a few connections fits a single
  account; a reactor multiplexing thousands of connections is gateway scale
  (pimgate/carillon), out of scope here.
- **Pool size is bounded by server connection caps** (~a handful), so the win is
  overlap, not unbounded fan-out.
