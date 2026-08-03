---
cairn: tasks
change: guidelines-alignment
---

# Tasks

- [x] README.md: retitle to I/O replica, drop feature-gate wording and the cargo-features tip
- [x] CONTRIBUTING.md: retitle, add the cairn/ reading-order item, replace the feature matrix with the no_std note
- [x] CHANGELOG.md: backfill Unreleased from the cairn log since 0.1.0
- [x] Cargo.toml: drop the [features] table, tighten the proptest dev-dependency
- [x] lib.rs: unconditional client module, no extern crate std, header rewrite
- [x] coroutine.rs: compact ReplicaYield variants, prose repair
- [x] open/upgrade/mutate/sync/rekey: drop top-of-resume trace and State Display, Replica error prefixes, NOTE tags, prose repair
- [x] client.rs: core/alloc imports, core::error::Error, Storage/Remote variants, message rewording
- [x] tests/client.rs: follow the variant and message changes
- [x] build, test, fmt via the nix devshell
- [x] fold delta, write the log entry, archive the change
