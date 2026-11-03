#!/usr/bin/env bash
# Downloads the browsers the signers comparison needs (docs/runbooks/tks-signers.md)
# into ~/tks-browsers, for this user only: nothing is installed system-wide and
# nothing runs. Both are free:
#   - Lightpanda, its nightly build (github.com/lightpanda-io/browser);
#   - chrome-headless-shell, Google's old headless Chrome, the current Stable
#     build from Chrome for Testing.
# Remove them with: rm -rf ~/tks-browsers
set -euo pipefail

dir="${TKS_BROWSERS_DIR:-$HOME/tks-browsers}"
if [ "$(uname -sm)" != "Linux x86_64" ]; then
	echo "tks-signers-browsers: Linux x86-64 only (this is $(uname -sm))" >&2
	exit 1
fi
mkdir -p "$dir"
cd "$dir"

echo "== Lightpanda (nightly) =="
curl -fsSL --retry 3 -o lightpanda.download https://github.com/lightpanda-io/browser/releases/download/nightly/lightpanda-x86_64-linux
chmod +x lightpanda.download
mv lightpanda.download lightpanda
echo "version: $(LIGHTPANDA_DISABLE_TELEMETRY=true ./lightpanda version)"
sha256sum lightpanda

echo
echo "== chrome-headless-shell (Chrome for Testing, Stable) =="
url=$(curl -fsSL --retry 3 https://googlechromelabs.github.io/chrome-for-testing/last-known-good-versions-with-downloads.json |
	python3 -c 'import json, sys
s = json.load(sys.stdin)["channels"]["Stable"]
print(next(d["url"] for d in s["downloads"]["chrome-headless-shell"] if d["platform"] == "linux64"))')
curl -fsSL --retry 3 -o chrome-headless-shell.zip "$url"
rm -rf chrome-headless-shell-linux64
python3 -m zipfile -e chrome-headless-shell.zip .
rm chrome-headless-shell.zip
chmod +x chrome-headless-shell-linux64/chrome-headless-shell
[ -f chrome-headless-shell-linux64/chrome_crashpad_handler ] && chmod +x chrome-headless-shell-linux64/chrome_crashpad_handler
missing=$(ldd chrome-headless-shell-linux64/chrome-headless-shell | grep "not found" || true)
if [ -n "$missing" ]; then
	echo "chrome-headless-shell needs these libraries, which Chrome also needs:" >&2
	echo "$missing" >&2
	exit 1
fi
echo "version: $(chrome-headless-shell-linux64/chrome-headless-shell --version)"
sha256sum chrome-headless-shell-linux64/chrome-headless-shell

echo
echo "tks-signers-browsers: both are in $dir"
