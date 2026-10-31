#!/usr/bin/env bash
# Moves a Go package to a new directory and rewrites every import of it, as
# a pure move: no other edit (ADR 0004 §7). Re-run it on a newer main
# instead of resolving conflicts by hand.
#
#   scripts/move-pkg.sh internal/fb/transport internal/scrape/transport
#
# The package keeps its name; gofmt re-sorts the import blocks.
set -euo pipefail
cd "$(dirname "$0")/.."

[[ $# -eq 2 ]] || { echo "usage: $0 <from-dir> <to-dir>" >&2; exit 2; }
from=${1%/} to=${2%/}
mod=$(go list -m)

[[ -d $from ]] || { echo "no such directory: $from" >&2; exit 1; }
[[ ! -e $to ]] || { echo "already exists: $to" >&2; exit 1; }
if [[ "$(basename "$from")" != "$(basename "$to")" ]]; then
	echo "the package name must not change: $(basename "$from") -> $(basename "$to")" >&2
	exit 1
fi

mkdir -p "$(dirname "$to")"
git mv "$from" "$to"

# Every quoted import of the package, or of a package below it.
git grep -lz -e "\"$mod/$from\"" -e "\"$mod/$from/" -- '*.go' |
	xargs -0 -r perl -pi -e "s#\"\Q$mod/$from\E(/[^\"]*)?\"#\"$mod/$to\$1\"#g"

gofmt -w $(git ls-files '*.go' | grep -v '^gen/')
go build ./...
echo "moved $from -> $to"
