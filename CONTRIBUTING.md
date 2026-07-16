# Contributing guide

Thank you for investing your time in contributing to I/O offline.

Whether you are a human or an AI agent, read these in order before touching the code:

1. the [Pimalaya README](https://github.com/pimalaya) for what the project is and how its repositories stack;
2. the [Pimalaya CONTRIBUTING](https://github.com/pimalaya/.github/blob/master/CONTRIBUTING.md) guide, which chains to the shared architecture and guidelines;
3. the inline header documentation, starting with src/lib.rs: it is the architecture document of this crate;
4. the docs/ folder for the development history and living plans.

Everything below documents only what differs from the Pimalaya standards.

## Feature matrix

io-offline is a no_std library (with alloc). The I/O-free coroutine core is always present; the blocking driver sits behind the client feature (on by default), which is the only thing pulling in std. Build both ways so no std-only code leaks into the always-on core:

```sh
cargo build --no-default-features    # I/O-free coroutine core, no std leak
cargo build                          # core plus the std client (default)
```

## Tests

Each coroutine ships the canonical unit-test layout: every transition, plus the missing-argument and unexpected-argument error arms. Scenario tests over a scripted storage and remote live in [tests/integration.rs](./tests/integration.rs), the proptest invariants (flag-merge symmetry, random-interleaving convergence, arbitrary-argument robustness) in [tests/property.rs](./tests/property.rs), and the std client is exercised in [tests/client.rs](./tests/client.rs).
