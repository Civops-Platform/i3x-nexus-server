#!/usr/bin/env bash
# Run CESMII's i3X 1.0 conformance suite against a fresh nexus.
#
#   scripts/conformance.sh [config.json] [runs]
#
# Starts the nexus on a throwaway data dir with a conformance principal that owns
# the config's writable namespaces, runs the suite `runs` times (default 3), and
# stops it. The suite's echo writes land in the throwaway dir and vanish with it.
# Suite location: $I3X_SUITE, default ~/i3x-suite/conformance-tests.
set -euo pipefail
cd "$(dirname "$0")/.."
CONFIG=${1:-config/example-site.json}
RUNS=${2:-3}
SUITE=${I3X_SUITE:-$HOME/i3x-suite/conformance-tests}
PORT=${PORT:-8409}
TMP=$(mktemp -d)
trap 'kill $PID 2>/dev/null || true; rm -rf "$TMP"' EXIT

OWNS=$(node -e 'const c=require(process.argv[1]);console.log(JSON.stringify([...new Set(c.types.filter(t=>t.write==="W0").map(t=>t.namespaceUri))]))' "$PWD/$CONFIG")
# One principal per run: the per-principal rate limit (25/s, burst 120) is real, and
# three back-to-back runs on one token would measure the limiter, not the server.
TOKENS=()
ENTRIES=()
for r in $(seq 1 "$RUNS"); do
  T=$(openssl rand -hex 24)
  TOKENS+=("$T")
  H=$(printf %s "$T" | shasum -a 256 | cut -d' ' -f1)
  ENTRIES+=("{\"id\":\"conformance-$r\",\"kind\":\"service\",\"scopes\":[\"read\",\"media:read\",\"media:upload\"],\"owns\":$OWNS,\"tokenSha256\":\"$H\"}")
done
(IFS=,; echo "[${ENTRIES[*]}]") > "$TMP/principals.json"

# A config with systems of record (tier W1) gets the synthetic CMMS it points at.
if node -e 'process.exit(require(process.argv[1]).systems?.length ? 0 : 1)' "$PWD/$CONFIG"; then
  node --import tsx scripts/synthetic-cmms.ts 8430 > "$TMP/cmms.log" 2>&1 & CMMS=$!
  trap 'kill $PID $CMMS 2>/dev/null || true; rm -rf "$TMP"' EXIT
  sleep 1
fi

# TLS=1: serve HTTPS with a throwaway CA the suite is told to trust (no --insecure).
SCHEME=http; TLSENV=""
if [ -n "${TLS:-}" ]; then
  openssl req -x509 -newkey rsa:2048 -nodes -keyout "$TMP/ca.key" -out "$TMP/ca.crt" -days 1 -subj "/CN=conformance CA" 2>/dev/null
  openssl req -newkey rsa:2048 -nodes -keyout "$TMP/srv.key" -out "$TMP/srv.csr" -subj "/CN=localhost" 2>/dev/null
  printf 'subjectAltName=IP:127.0.0.1,DNS:localhost\nextendedKeyUsage=serverAuth\n' > "$TMP/srv.ext"
  openssl x509 -req -in "$TMP/srv.csr" -CA "$TMP/ca.crt" -CAkey "$TMP/ca.key" -CAcreateserial -out "$TMP/srv.crt" -days 1 -extfile "$TMP/srv.ext" 2>/dev/null
  SCHEME=https; TLSENV="NEXUS_TLS_CERT=$TMP/srv.crt NEXUS_TLS_KEY=$TMP/srv.key"
  export NODE_EXTRA_CA_CERTS="$TMP/ca.crt"
fi
env $TLSENV NEXUS_CONFIG=$CONFIG NEXUS_PRINCIPALS=$TMP/principals.json NEXUS_DATA=$TMP/data NEXUS_PORT=$PORT \
  node --import tsx src/main.ts > "$TMP/nexus.log" 2>&1 &
PID=$!
for i in $(seq 1 40); do curl -sf --cacert "$TMP/ca.crt" "$SCHEME://127.0.0.1:$PORT/v1/info" >/dev/null 2>&1 || curl -sf "$SCHEME://127.0.0.1:$PORT/v1/info" >/dev/null 2>&1 && break; sleep 0.5; done
curl -sfk "$SCHEME://127.0.0.1:$PORT/v1/info" >/dev/null || { cat "$TMP/nexus.log"; exit 1; }

mkdir -p docs/conformance
for r in $(seq 1 "$RUNS"); do
  echo "── run $r of $RUNS"
  node "$SUITE/bin/i3x-test.js" run "$SCHEME://127.0.0.1:$PORT/v1" --token "${TOKENS[$((r-1))]}" --json "docs/conformance/run-$r.json" 2>&1 | grep -E "Result:|passed|✗" || true
done
