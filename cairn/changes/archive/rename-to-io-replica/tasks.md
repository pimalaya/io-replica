---
cairn: tasks
change: rename-to-io-replica
---

- [x] Rename `Offline*` types to `Replica*` across io-offline src + tests
- [x] Rename crate `io-offline` -> `io-replica` (Cargo.toml, docs, README, CHANGELOG)
- [x] Update cairn spec/ to the new type names (leave log/ + archive/ as history)
- [x] Rename the local directory io-offline -> io-replica
- [x] Update neverest (dep, patch path, `io_offline`/`Offline*` in src/offline/)
- [x] Update cardamum-android Rust (io_offline/Offline*, path patch)
- [x] Update himalaya-android-m3 Rust (io_offline/Offline*, path patch)
- [x] Build + test io-replica; build neverest
- [x] Land: note the rename in the log
