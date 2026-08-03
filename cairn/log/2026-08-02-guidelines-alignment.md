---
cairn: log
change: guidelines-alignment
landed: 2026-08-02
---

# Pimalaya guidelines alignment

A line-by-line pass over .github/GUIDELINES.md, plus the stale-branding fallout of the io-offline to io-replica rename.

Markdown: README and CONTRIBUTING retitled to I/O replica (readme-004); CONTRIBUTING gained the cairn/ reading-order item and its feature matrix became the "no cargo features" note (contributing-002); the empty Unreleased CHANGELOG section was backfilled with the net changes landed since 0.1.0, sourced from this log (changelog-001/002).

Code: the `client` feature was removed under the crate-003 golden rule. client.rs now imports from core and alloc only (crate-004) and implements `core::error::Error`, so the driver pulls no crates at all; the gate guarded nothing and the module ships unconditionally, making the crate feature-free (the same conclusion hub.md already recorded for the hub). `ReplicaClientError` variants `ReplicaStorage`/`ReplicaRemote` became `Storage`/`Remote`, and error prefixes moved from `Offline <VERB> failed` to `Replica <VERB> failed`, matching io-imap's shape (naming-012). The top-of-resume `trace!` in the five verbs was dropped per logging-002 along with the now-unused `State` Display impls; state changes keep logging at the end of match arms. `ReplicaYield` variants lost their separating blank lines (inline-003). Untagged production comment blocks were tagged NOTE (inline-004); test narration keeps untagged scenario comments, matching io-imap. Doc prose damaged by the mechanical `Replica` prefix rename was repaired (`Local, ReplicaBase and ReplicaRemote`, `ReplicaFlags merge element-wise`, and similar). The proptest dev-dependency was tightened to `default-features = false, features = ["std"]` (cargo-008).

No spec requirement moved: the merge and seam behaviour is untouched, so the delta was empty. Verified with cargo build (default and --no-default-features, now equivalent), cargo test (146 tests across all targets), cargo clippy --all-targets and cargo fmt, all through the nix devshell.
