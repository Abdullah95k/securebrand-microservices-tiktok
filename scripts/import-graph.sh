#!/usr/bin/env bash
# Prints the in-module import graph that docs/instagram/reuse-matrix.md is
# built on: one line per package with its direct in-module imports, then the
# number of in-module packages each binary links. Re-run it after a move to
# check the matrix still matches the code.
#
#   scripts/import-graph.sh            # graph + binaries
#   scripts/import-graph.sh internal/fb  # only packages under a prefix
set -euo pipefail
cd "$(dirname "$0")/.."

mod=$(go list -m)
prefix=${1:-}

echo "# direct in-module imports (${mod})"
go list -f '{{.ImportPath}}|{{join .Imports " "}}' ./internal/... ./cmd/... |
	sed "s#${mod}/##g" |
	awk -F'|' -v p="$prefix" '
		index($1, p) == 1 {
			n = split($2, a, " "); s = ""
			for (i = 1; i <= n; i++) if (a[i] ~ /^(internal|gen|api|configs|migrations)\//) s = s " " a[i]
			print $1 ":" s
		}'

if [[ -z "$prefix" ]]; then
	echo
	echo "# in-module packages linked by each binary (go list -deps)"
	for c in $(go list ./cmd/...); do
		n=$(go list -deps "$c" | grep -c "^${mod}/internal/" || true)
		echo "${c#"${mod}"/} ${n}"
	done
fi
