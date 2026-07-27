#!/usr/bin/env bash
#
# Shared helpers for the Cairn bash reference port. Sourced, not executed.
# This port is OPTIONAL. Cairn needs no tooling. See ../../GUIDE.md.

die() { printf 'cairn: %s\n' "$*" >&2; exit 2; }

# Walk up from $1 to the nearest ancestor holding a `cairn/` dir or `cairn.toml`.
find_root() {
  local dir; dir=$(cd "$1" 2>/dev/null && pwd) || return 1
  while :; do
    if [ -d "$dir/cairn" ] || [ -f "$dir/cairn.toml" ]; then
      printf '%s\n' "$dir"; return 0
    fi
    [ "$dir" = "/" ] && return 1
    dir=$(dirname "$dir")
  done
}

# Print the leading YAML frontmatter block, between the first two `---`.
# Emits nothing when the file does not open with `---`.
fm_block() {
  awk 'NR==1 && $0 != "---" { exit }
       /^---[[:space:]]*$/  { c++; if (c==2) exit; next }
       c==1 { print }' "$1" 2>/dev/null
}

# Value of frontmatter key $2 in file $1. Inline `# comments` are stripped.
fm_get() {
  fm_block "$1" | sed -n "s/^$2:[[:space:]]*//p" | head -n1 \
    | sed -e 's/[[:space:]]*#.*$//' -e 's/[[:space:]]*$//'
}

# The `cairn:` type of file $1, or empty.
fm_type_of() { fm_block "$1" | sed -n 's/^cairn:[[:space:]]*//p' | head -n1; }

# Scalar key $2 from cairn.toml $1, default $3. Subset TOML: key = "v" or bare.
toml_get() {
  local file="$1" key="$2" def="$3" val
  [ -f "$file" ] || { printf '%s\n' "$def"; return; }
  val=$(sed -n "s/^[[:space:]]*$key[[:space:]]*=[[:space:]]*//p" "$file" | head -n1)
  [ -z "$val" ] && { printf '%s\n' "$def"; return; }
  val=${val%%#*}
  val=$(printf '%s' "$val" | sed -e 's/[[:space:]]*$//' -e 's/^"//' -e 's/"$//')
  printf '%s\n' "$val"
}

is_kebab() { printf '%s' "$1" | grep -qE '^[a-z0-9][a-z0-9-]*$'; }

# List change directories under $1, excluding archive/, plus archive/* entries.
change_dirs() {
  local changes="$1" d b
  [ -d "$changes" ] || return 0
  for d in "$changes"/*/; do
    [ -d "$d" ] || continue
    b=$(basename "$d")
    [ "$b" = "archive" ] && continue
    printf '%s\n' "${d%/}"
  done
  if [ -d "$changes/archive" ]; then
    for d in "$changes/archive"/*/; do
      [ -d "$d" ] || continue
      printf '%s\n' "${d%/}"
    done
  fi
}
