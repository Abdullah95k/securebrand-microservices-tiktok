#!/usr/bin/env bash
# A6 (M16): the previous release's code must run on the new schema, so a
# rollback (docs/runbooks/tks-rollback.md) needs no down migration:
# migrations only add (columns nullable or with defaults, new tables,
# indexes), never rename, drop or tighten what released code uses.
#
# Migrates template1 with this checkout's migrations, so every database
# the old code's tests create starts with the new schema (their own
# `migrate up` then has nothing to do), and runs the old code's end-to-end
# suite against it. Needs TKS_TEST_PG_DSN (an admin DSN; the server must be
# the test's own: template1 is changed and restored) and the other
# TKS_TEST_* variables of the e2e suite.
#
# Usage: scripts/check-schema-compat.sh OLD_REF [go test package pattern]
set -euo pipefail
old=${1:?usage: check-schema-compat.sh OLD_REF [packages]}
pkgs=${2:-./internal/tt/e2e/...}
: "${TKS_TEST_PG_DSN:?TKS_TEST_PG_DSN must be set}"

if git diff --quiet "$old" -- migrations/ && [ -z "$(git ls-files --others --exclude-standard migrations/)" ]; then
  echo "schema-compat: no migration changed since $old; nothing to check"
  exit 0
fi
echo "schema-compat: migrations changed since $old:"
git diff --stat "$old" -- migrations/; git ls-files --others --exclude-standard migrations/

# The admin DSN with database template1.
tmpl=$(python3 -c 'import sys,urllib.parse as u; p=u.urlsplit(sys.argv[1]); print(u.urlunsplit(p._replace(path="/template1")))' "$TKS_TEST_PG_DSN")
work=$(mktemp -d)
cleanup() {
  PGOPTIONS='-c client_min_messages=warning' psql -q "$tmpl" -c 'DROP SCHEMA IF EXISTS tt CASCADE' >/dev/null || true
  git worktree remove --force "$work/old" >/dev/null 2>&1 || true
  rm -rf "$work"
}
trap cleanup EXIT

TKS_POSTGRES__DSN="$tmpl" go run ./cmd/tksctl migrate up >/dev/null
echo "schema-compat: template1 migrated to $(psql -tA "$tmpl" -c 'SELECT max(version_id) FROM tt.goose_db_version')"

git worktree add --detach "$work/old" "$old" >/dev/null
echo "schema-compat: running $old's $pkgs on the new schema"
(cd "$work/old" && go test -count=1 -timeout 25m $pkgs)
echo "schema-compat: $old's code runs on the new schema"
