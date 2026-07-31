---
cairn: delta
change: concurrent-size-ordered-fetch
---

## ADDED Requirements

### Requirement: Fetch batches are order-independent
A `WantsFetch` batch SHALL impose no ordering on its handles: the consumer MAY
fetch them in any order and concurrently, and SHALL return results keyed by
handle. The engine SHALL match fetched results by handle, not by position, so a
consumer servicing the batch across a connection pool is correct.

#### Scenario: Largest-first hydration overlaps a heavy message
- GIVEN an enumerate that reports member sizes and a consumer that fetches a Full batch across a bounded connection pool, largest first
- WHEN the batch is hydrated
- THEN the heavy member is fetched concurrently with the light ones, results are matched by handle, and no fetch order is assumed

## MODIFIED Requirements

(none)

## REMOVED Requirements

(none)
