# Contributing guide

Thank you for investing your time in contributing to io-offline.

Whether you are a human or an AI agent, read these in order before touching the code:

1. the [Pimalaya README](https://github.com/pimalaya) for what the project is and how its repositories stack;
2. the [Pimalaya ARCHITECTURE](https://github.com/pimalaya/.github/blob/master/ARCHITECTURE.md) for the conventions every repository shares (layering, `no_std`, modules, errors, code style, licensing, notes for AI agents);
3. this repository's [ARCHITECTURE.md](./ARCHITECTURE.md) for the replica engine's model, verbs and merge semantics;
4. this guide, for how to build, test and submit changes here.

## Development environment

The environment is managed by [Nix](https://nixos.org/download.html). `nix develop` spawns a shell with the right toolchain; every cargo command below assumes it (or prefix them with `nix develop --command`).

Without Nix, install a recent stable toolchain via [rustup](https://rust-lang.github.io/rustup/) (`rustup update`); the crate needs Rust matching the `rust-version` in [Cargo.toml](./Cargo.toml).

## Build

io-offline is a `#![no_std]` library (with `alloc`). The I/O-free coroutine core is always present; the blocking driver sits behind the `client` feature (on by default), which is the only thing pulling in `std`.

```sh
cargo build                                  # default features (client)
cargo build --no-default-features            # I/O-free core, no std leak
cargo build --release --all-features
```

When touching feature gates or imports, build with and without `client` so no `std`-only code leaks into the always-on core.

## Lint, test, audit

```sh
cargo test --all-features                    # unit + integration + property tests
cargo clippy --all-targets --all-features
cargo fmt                                    # CI checks `cargo fmt --check`
cargo deny check                             # licenses, advisories, sources
```

Before opening a PR, make sure `cargo test`, `cargo clippy`, `cargo fmt --check` and `cargo deny check` pass.

Each coroutine ships the canonical unit-test layout (every transition plus the missing-arg and unexpected-arg error arms); scenario tests over a scripted storage and remote live in [tests/integration.rs](tests/integration.rs), proptest invariants (flag-merge symmetry, random-interleaving convergence, arbitrary-arg robustness) in [tests/property.rs](tests/property.rs), and the std client is exercised in [tests/client.rs](tests/client.rs).

## Commit style

io-offline follows the [conventional commits specification](https://www.conventionalcommits.org/en/v1.0.0/#summary). Keep the subject imperative and scoped; describe the *why* in the body when it is not obvious.
