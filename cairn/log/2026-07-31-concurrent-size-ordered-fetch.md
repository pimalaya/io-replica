---
cairn: log
change: concurrent-size-ordered-fetch
landed: 2026-07-31
---

# Concurrent, size-ordered hydration (io-replica part)

Guaranteed that a `WantsFetch` batch is order-independent so a consumer may
service it across a connection pool. The upgrade coroutine already matched
fetched results by handle (`self.placements.get(&item.handle)`), so this landed
as a spec guarantee plus a unit test — `fetch_results_are_matched_by_handle_not_order`:
a two-handle Full fetch whose results are returned in reverse order still lands
each object on its own placement. No engine code changed; all suites and fmt
pass.

Dropped, during implementation, the proposed spine `size` field on
`ReplicaRemoteItem`: the engine never schedules, and the scheduling consumer
(neverest) already knows sizes from its own enumerate, so size stays a consumer
concern rather than an unused spine field. The neverest change of the same id
implements the pool and largest-first ordering.

Spec updated: `sync` (ADDED: fetch batches are order-independent).
