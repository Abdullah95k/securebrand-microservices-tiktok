#!/usr/bin/env bash
# Checks that every relative Markdown link in the repository's tracked .md
# files points at an existing file (anchors and external links are not
# checked).
set -uo pipefail
cd "$(dirname "$0")/.."
broken=0
while IFS= read -r f; do
  dir=$(dirname "$f")
  while IFS= read -r t; do
    case "$t" in
      ''|http://*|https://*|mailto:*|\#*) continue ;;
    esac
    path=${t%%#*}
    if [ ! -e "$dir/$path" ]; then
      echo "$f: broken link: $t"
      broken=1
    fi
  done < <(grep -oE '\]\([^)]+\)' "$f" | sed -E 's/^\]\((.*)\)$/\1/' | sed -E 's/ +"[^"]*"$//')
done < <(git ls-files '*.md')
exit $broken
