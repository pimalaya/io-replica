# Reference implementations

Everything Cairn needs can be done **by hand**, see [`../GUIDE.md`](../GUIDE.md). The ports here are optional conveniences that mechanise the same scaffold and the same conformance rules ([`../CAIRN.md`](../CAIRN.md) §8), for CI or for humans who prefer a command. None of them is required, and none is privileged over the others.

## Available

| Language | Path              | Runtime          | Layout                        |
| -------- | ----------------- | ---------------- | ----------------------------- |
| bash     | [`bash/`](./bash) | bash + coreutils | `init`, `verify`, `common.sh` |

Each port provides two scoped commands:

- `init [dir]` scaffolds a `cairn/` root (`spec/`, `changes/`, `log/`).
- `verify [dir]` checks conformance and exits non-zero on violations. Read-only.

## Adding a port

A conforming port implements exactly the checks C1-C9 from CAIRN.md §8 and the `init` scaffold from GUIDE.md. Keep it dependency-light. Resolve the root by walking up to the nearest `cairn/` or `cairn.toml`. Treat `verify` as strictly read-only. Add a row above and a short `README.md` in the language folder describing how to run it.
