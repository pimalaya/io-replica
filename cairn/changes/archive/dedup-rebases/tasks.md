---
cairn: tasks
change: dedup-rebases
---

# Tasks

- [x] Rebase `base.object` in the dedup branch of `ReplicaUpgrade`.
- [x] `is_mutable`: leave a revision-carrying placement out of the link lookup
      and ignore a hit on it, so it is fetched.
- [x] Regression tests: a deduped body rebases so the placement reads clean; a
      mutable placement is fetched rather than linked. Both verified failing
      against the code before the fix.
- [x] `cargo test`, `cargo clippy --all-targets`, `cargo fmt`.
- [x] Verified against the live Fastmail account through neverest: the phantom
      `update item` hunk is gone and a resynced store converges on the first
      run, dedup still saving the second download.
- [x] Fold `delta.md` into `cairn/spec/sync.md`; add the `cairn/log` entry; mark
      the change `landed` and archive it.
