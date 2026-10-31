#!/usr/bin/env bash
# Checks that every relative Markdown link in docs/ and the README points at
# an existing file (anchors and external links are not checked).
set -euo pipefail
cd "$(dirname "$0")/.."
status=0
while IFS= read -r f; do
  dir=$(dirname "$f")
  # Link targets: ](target) with no scheme, not a pure anchor.
  grep -oE '\]\([^)#][^)]*\)' "$f" | sed -E 's/^\]\((.*)\)$/\1/' | while IFS= read -r t; do
    case "$t" in
      http://*|https://*|mailto:*) continue ;;
    esac
    path=${t%%#*}
    [ -z "$path" ] && continue
    if [ ! -e "$dir/$path" ]; then
      echo "$f: broken link: $t"
      echo broken >> /tmp/doc-links.$$
    fi
  done
done < <(git ls-files '*.md')
if [ -f /tmp/doc-links.$$ ]; then rm -f /tmp/doc-links.$$; status=1; fi
exit $status
