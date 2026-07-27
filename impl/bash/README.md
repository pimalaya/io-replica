# Cairn, bash port

An optional reference implementation of the Cairn convention in bash (bash plus coreutils). Cairn needs no tooling, see [`../../GUIDE.md`](../../GUIDE.md). This port is a convenience for CI or a quick command.

## Scripts

- [`init`](./init) scaffolds a `cairn/` root. Usage: `init [dir] [--base NAME]`.
- [`verify`](./verify) checks conformance (CAIRN.md §8), read-only. Usage: `verify [dir] [--block]`. Exit 0 when conformant, 1 on violations, 2 with `--block`.
- `common.sh` holds shared helpers, sourced by both. It is not executed directly.

## Use

```sh
impl/bash/init             # create cairn/ under the current directory
impl/bash/verify           # check the tree, exit non-zero on violations
impl/bash/verify --block   # exit 2 on violations, for a Claude Code Stop hook
```

Both resolve the Cairn root by walking up from the given directory (default: current) to the nearest `cairn/` or `cairn.toml`.
