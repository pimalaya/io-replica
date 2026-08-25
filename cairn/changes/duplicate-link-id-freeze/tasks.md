---
cairn: tasks
change: duplicate-link-id-freeze
---

# Tasks

- [x] `ReplicaSourceBinding::ambiguous_handles` and `ReplicaStatus::Ambiguous`,
      documented as the identity-axis twin of `conflicted` / `Conflict`.
- [x] `ReplicaUpgrade`: applying a fetched link id another placement of the
      collection already holds records the handle as ambiguous instead of
      linking, beside the existing not-yet-linked rule (upgrade.rs, the
      `PendingFetch` arm that assigns `link_id`).
- [x] `ReplicaHub::project`: a binding carrying ambiguous handles projects
      `Ambiguous`; `absorb` round-trips the handles, and clears them when an
      upsert carries none.
- [x] `ReplicaSync`: an `Ambiguous` placement derives no push, and its absence
      from a complete snapshot derives no vanish-delete. An enumeration
      reporting the identity once clears it.
- [x] `ReplicaMutate` and `ReplicaRekey`: refuse to stage against an `Ambiguous`
      placement, and carry the state over a handle-space change untouched.
- [x] Tests, each asserting the failure the reproduction showed:
      - two handles of one collection resolving to one link id leave the second
        unlinked and the first ambiguous;
      - an ambiguous placement missing from a complete snapshot derives no
        delete;
      - a hub holding it projects it to no other source and propagates no
        delete;
      - an enumeration reporting the identity once clears the state and syncing
        resumes;
      - a rekey carries the ambiguity over.
- [x] `cargo test`, `cargo clippy --all-targets`, `cargo fmt`.
- [x] CHANGELOG under `### Changed` for the two breaks and `### Fixed` for the
      loss. The 0.5.0 release prep itself belongs to the release, not here.
- [x] Fold `delta.md` into `cairn/spec/sync.md` (detection, the derive-nothing
      rules) and `cairn/spec/hub.md` (projection, propagation); add the
      `cairn/log` entry; mark the change `landed` and archive it.
- [ ] Hand over to io-pimdir `duplicate-link-id-freeze` (persistence), then
      neverest `duplicate-link-id-freeze` (report + end-to-end proof).
