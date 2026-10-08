#!/usr/bin/env bash
# All portal suites. Exit non-zero on any failure. CI and the zip builder both call this.
set -euo pipefail
cd "$(dirname "$0")"
node --check ../src/index.js && (cd ../../relay && node build.mjs >/dev/null) && git -C ../.. diff --quiet -- relay/src/index.js || { echo "!! relay/src/index.js is stale: run relay/build.mjs and commit"; exit 1; }
for t in sec-test sec-thread-test m15-test crawl-test a2a-conformance identity-test relay-test fly-test install-test seed-test ghosts-test tribe-test thread-test moved-test box-test cli-test kv-budget-test do-store-test; do
  echo "== $t"
  out=$(node "$t.mjs" 2>&1 | grep -vE 'MODULE_TYPELESS|Reparsing|eliminate this|trace-warnings') || true
  echo "$out" | grep -E 'passed|skipping|^FAIL' || true
  if ! echo "$out" | grep -qE "[0-9]+ passed, 0 failed|skipping" || echo "$out" | grep -qE "^FAIL|[1-9][0-9]* failed"; then echo "$out" | grep -E '^FAIL|Error' | head; echo "!! $t failed"; exit 1; fi
done
# Fixture drift: the committed fixtures must equal what the Worker emits now. Rust round-trips the committed ones.
echo "== fixtures"
tmp=$(mktemp -d); (cd "$tmp" && mkdir fixtures && cp "$OLDPWD"/write-fixtures.mjs . && sed -i.bak "s#'../src/index.js'#'$OLDPWD/../src/index.js'#" write-fixtures.mjs && node write-fixtures.mjs >/dev/null 2>&1)
mask() { sed -E 's/[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}|[0-9a-f]{32}/ID/g; s/"(publicKey|oldKey|newKey|sig|newSig|kid|keyId|oldKid|newKid|timestamp|at|castAt|createdAt)": "[^"]*"/"\1": "X"/g' "$1"; }
for f in agent-card send-message ack actions pulse-config handle-record rotation-record cast; do
  # ids are random per run; compare with ids masked
  if ! diff <(mask "fixtures/$f.json") <(mask "$tmp/fixtures/$f.json") >/dev/null; then echo "!! fixture drift: portal/test/fixtures/$f.json no longer matches the Worker. Re-run: node write-fixtures.mjs"; rm -rf "$tmp"; exit 1; fi
done; rm -rf "$tmp"; echo "fixtures match the Worker"
echo "all suites green"
