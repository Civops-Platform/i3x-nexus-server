// Verify a nexus ledger in Postgres: hash chain and idempotency keys.  npx tsx scripts/verify-ledger.ts <postgres-url>
import pg from "pg";
import { verifyChain } from "../src/core/records";
const c = new pg.Client(process.argv[2]); await c.connect();
const r = await c.query("SELECT * FROM nexus_records ORDER BY seq");
const v = r.rows.map((x) => ({ elementId: x.element_id, version: Number(x.version), value: x.value, timestamp: x.ts.toISOString(), writtenAt: x.written_at.toISOString(), principal: x.principal, prevHash: x.prev_hash, hash: x.hash }));
console.log(`ledger rows ${v.length}; chain ${JSON.stringify(verifyChain(v) ?? "intact")}; duplicate idempotency keys ${(await c.query("select count(*) - count(distinct idempotency_key) n from nexus_records where idempotency_key is not null")).rows[0].n}`);
await c.end();
