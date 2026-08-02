---
cairn: tasks
change: fetch-keeps-resolved-link
---

- [x] Upgrade fetch-apply sets `link_id` from the fetch only when the placement
      has none; an already-linked item keeps its link and rises to `Full`.
- [x] Test: a `Full` fetch with a divergent link keeps the original link, level Full.
- [x] Test: a `Meta` fetch of an unlinked item still takes the fetched link.
- [x] Full test suite green (130 unit tests); no test relied on the overwrite.
- [x] Downstream (neverest) live regression: no ghosts, alt-link (no-Message-ID)
      messages reach Full, idempotent re-sync.
- [ ] Fold delta into `cairn/spec/sync.md`; write log entry.
