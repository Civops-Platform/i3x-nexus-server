#!/usr/bin/env bash
# Profile Level 2: the CESMII suite runs against node B while node A, the governor,
# is killed hard (SIGKILL) part-way through. Passing means a client on the surviving
# node sees no failure across the takeover.
#
#   NEXUS_TEST_PG=postgres://… scripts/conformance-failover.sh [config.json]
set -euo pipefail
cd "$(dirname "$0")/.."
CONFIG=${1:-config/example-site.json}
SUITE=${I3X_SUITE:-$HOME/i3x-suite/conformance-tests}
PGURL=${NEXUS_TEST_PG:-$(cat .local/pg-url)}
SCHEMA=cf_$(openssl rand -hex 4)
TMP=$(mktemp -d)
cleanup() { kill -9 ${PA:-} ${PB:-} 2>/dev/null || true; node -e "const pg=require('pg');const c=new pg.Client(process.argv[1]);c.connect().then(()=>c.query('DROP SCHEMA IF EXISTS $SCHEMA CASCADE')).then(()=>c.end())" "$PGURL" || true; rm -rf "$TMP"; }
trap cleanup EXIT
node -e "const pg=require('pg');const c=new pg.Client(process.argv[1]);c.connect().then(()=>c.query('CREATE SCHEMA $SCHEMA')).then(()=>c.end())" "$PGURL"
URL=$(node -e "const u=new URL(process.argv[1]);u.searchParams.set('options','-c search_path=$SCHEMA');console.log(u.toString())" "$PGURL")

TOKEN=$(openssl rand -hex 24); HASH=$(printf %s "$TOKEN" | shasum -a 256 | cut -d' ' -f1)
OWNS=$(node -e 'const c=require(process.argv[1]);console.log(JSON.stringify([...new Set(c.types.filter(t=>t.write==="W0").map(t=>t.namespaceUri))]))' "$PWD/$CONFIG")
echo "[{\"id\":\"conformance\",\"kind\":\"service\",\"scopes\":[\"read\",\"media:read\",\"media:upload\"],\"owns\":$OWNS,\"tokenSha256\":\"$HASH\"}]" > "$TMP/p.json"
common="NEXUS_CONFIG=$CONFIG NEXUS_PRINCIPALS=$TMP/p.json NEXUS_PG_URL=$URL NEXUS_CLUSTER_SECRET=s NEXUS_MEDIA_KEY=k NEXUS_LEASE_TTL_MS=${TTL:-3000}"
env $common NEXUS_NODE_ID=a NEXUS_DATA=$TMP/a NEXUS_PORT=8411 node --import tsx src/main.ts > "$TMP/a.log" 2>&1 & PA=$!
sleep 3
env $common NEXUS_NODE_ID=b NEXUS_DATA=$TMP/b NEXUS_PORT=8412 node --import tsx src/main.ts > "$TMP/b.log" 2>&1 & PB=$!
for i in $(seq 1 40); do curl -sf http://127.0.0.1:8412/v1/info | grep -q '"state":"ok"' && break; sleep 0.5; done
curl -s http://127.0.0.1:8412/v1/info | node -e 'let s="";process.stdin.on("data",d=>s+=d).on("end",()=>{const r=JSON.parse(s).result["x-nexus"].redundancy;console.log("before:",r.state,"governor",r.governor)})'

( sleep "${KILL_AFTER:-1}"; echo "── SIGKILL node a (governor) at $(date +%T)"; kill -9 $PA ) &
node "$SUITE/bin/i3x-test.js" run http://127.0.0.1:8412/v1 --token "$TOKEN" --json docs/conformance/failover-run.json 2>&1 | grep -E "Result:|passed|✗" || true
echo "── suite finished at $(date +%T)"
sleep 8
curl -s http://127.0.0.1:8412/v1/info | node -e 'let s="";process.stdin.on("data",d=>s+=d).on("end",()=>{const r=JSON.parse(s).result["x-nexus"].redundancy;console.log("after:",r.state,"governor",r.governor,"role",r.role)})'
grep -c "" "$TMP/b.log" >/dev/null
