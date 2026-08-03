# Contributing guide

Thank you for investing your time in contributing to I/O replica.

Whether you are a human or an AI agent, read these in order before touching the code:

1. the [Pimalaya README](https://github.com/pimalaya) for what the project is and how its repositories stack;
2. the [Pimalaya CONTRIBUTING](https://github.com/pimalaya/.github/blob/master/CONTRIBUTING.md) guide, which chains to the shared architecture and guidelines;
3. the inline header documentation, starting with src/lib.rs: it is the architecture document of this crate;
4. the cairn/ folder for the development history and living plans (the Cairn convention: spec/, changes/, log/).

Everything below documents only what differs from the Pimalaya standards.

## No cargo features

io-replica is a no_std library (with alloc) end to end: even the blocking driver in src/client.rs performs no I/O of its own (blocking happens inside the consumer-implemented traits), so it pulls no extra crates and carries no feature gate. A plain `cargo build` checks everything; there is no feature matrix to walk.

## Tests

Each coroutine ships the canonical unit-test layout: every transition, plus the missing-argument and unexpected-argument error arms. Scenario tests over a scripted storage and remote live in [tests/integration.rs](./tests/integration.rs), the proptest invariants (flag-merge symmetry, random-interleaving convergence, arbitrary-argument robustness) in [tests/property.rs](./tests/property.rs), and the blocking client is exercised in [tests/client.rs](./tests/client.rs).
