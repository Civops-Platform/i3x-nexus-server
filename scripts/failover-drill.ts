// Profile Level 2 acceptance drill: 20 forced governor failures under write load.
//
// Two nexus processes share a throwaway Postgres schema. A client writes records
// continuously through whichever node answers, retrying each write with its own
// Idempotency-Key until acknowledged, and failing over between the endpoints the
// pair publishes. Each cycle SIGKILLs the current governor, measures the time until
// a write is acknowledged again (the write RTO the client sees), then restarts the
// killed node as the new standby. At the end the ledger is reconciled: every
// acknowledged write exactly once, nothing duplicated, the hash chain intact.
//
//   npx tsx scripts/failover-drill.ts [cycles=20] [ttlMs=10000]
// Writes docs/level2/failover-drill.json.

import { spawn, type ChildProcess } from "node:child_process";
import { mkdtempSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createHash, randomBytes } from "node:crypto";
import pg from "pg";
import { verifyChain, type RecordVersion } from "../src/core/records";

const CYCLES = Number(process.argv[2] ?? 20);
const TTL = Number(process.argv[3] ?? 10_000);
const PG = process.env.NEXUS_TEST_PG ?? readFileSync(".local/pg-url", "utf8").trim();
const schema = `drill_${randomBytes(4).toString("hex")}`;
const tmp = mkdtempSync(join(tmpdir(), "nexus-drill-"));
const token = randomBytes(24).toString("hex");
const cfg = JSON.parse(readFileSync("config/example-site.json", "utf8"));
const owns = [...new Set(cfg.types.filter((t: any) => t.write === "W0").map((t: any) => t.namespaceUri))];
writeFileSync(join(tmp, "p.json"), JSON.stringify([{ id: "drill", kind: "service", scopes: ["read"], owns, tokenSha256: createHash("sha256").update(token).digest("hex") }]));

const admin = new pg.Client(PG);
await admin.connect();
await admin.query(`CREATE SCHEMA ${schema}`);
const u = new URL(PG);
u.searchParams.set("options", `-c search_path=${schema}`);
const url = u.toString();

const ports: Record<string, number> = { a: 8421, b: 8422 };
const procs: Record<string, ChildProcess | null> = { a: null, b: null };
const start = (id: string) => {
  procs[id] = spawn(process.execPath, ["--import", "tsx", "src/main.ts"], {
    env: { ...process.env, NEXUS_CONFIG: "config/example-site.json", NEXUS_PRINCIPALS: join(tmp, "p.json"), NEXUS_DATA: join(tmp, id),
      NEXUS_PG_URL: url, NEXUS_NODE_ID: id, NEXUS_PORT: String(ports[id]), NEXUS_CLUSTER_SECRET: "drill", NEXUS_MEDIA_KEY: "drill", NEXUS_LEASE_TTL_MS: String(TTL) },
    stdio: "ignore",
  });
};
const base = (id: string) => `http://127.0.0.1:${ports[id]}/v1`;
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
const info = async (id: string) => {
  try { return (await (await fetch(`${base(id)}/info`, { signal: AbortSignal.timeout(2000) })).json() as any).result["x-nexus"].redundancy; } catch { return null; }
};

// The client: one write at a time, each retried with its key, rotating endpoints on failure.
const acked: Array<{ key: string; at: number }> = [];
let writing = true;
let seq = 0;
const writer = (async () => {
  let ep = 0;
  while (writing) {
    const key = `w${seq++}`;
    for (;;) {
      if (!writing) break;
      const id = ["a", "b"][ep % 2];
      try {
        const r = await fetch(`${base(id)}/objects/value`, {
          method: "PUT", signal: AbortSignal.timeout(TTL + 10_000),
          headers: { authorization: `Bearer ${token}`, "content-type": "application/json", "idempotency-key": key },
          body: JSON.stringify({ updates: [{ elementId: "signal:ship-eta", value: { value: { status: "active", order: key } } }] }),
        });
        const b = await r.json() as any;
        if (r.status === 200 && b.results?.[0]?.success) { acked.push({ key, at: Date.now() }); break; }
      } catch { /* node down */ }
      ep++;
      await sleep(100);
    }
    await sleep(50);
  }
})();

start("a"); await sleep(2500); start("b");
for (let i = 0; i < 60; i++) { const r = await info("b"); if (r?.state === "ok" && r.governor) break; await sleep(500); }

const cycles: Array<{ cycle: number; killed: string; rtoMs: number; takeoverMs: number }> = [];
for (let c = 1; c <= CYCLES; c++) {
  const gov = (await info("a"))?.governor ?? (await info("b"))?.governor;
  const victim = gov === "b" ? "b" : "a";
  const other = victim === "a" ? "b" : "a";
  await sleep(1500);
  const before = acked.length;
  const t0 = Date.now();
  procs[victim]!.kill("SIGKILL");
  let takeoverMs = -1;
  for (let i = 0; i < 400; i++) {
    const r = await info(other);
    if (r?.role === "governor") { takeoverMs = Date.now() - t0; break; }
    await sleep(100);
  }
  // Write RTO: first acknowledgement for a write issued after the kill.
  let rtoMs = -1;
  for (let i = 0; i < 600; i++) {
    const hit = acked.slice(before).find((a) => a.at > t0);
    if (hit) { rtoMs = hit.at - t0; break; }
    await sleep(100);
  }
  cycles.push({ cycle: c, killed: victim, rtoMs, takeoverMs });
  console.log(`cycle ${c}: killed ${victim}, governor takeover ${takeoverMs} ms, write RTO ${rtoMs} ms`);
  start(victim);
  for (let i = 0; i < 60; i++) { const r = await info(victim); if (r?.state === "ok") break; await sleep(500); }
}

writing = false;
await writer;
for (const p of Object.values(procs)) p?.kill("SIGKILL");

const rows = (await admin.query(`SELECT * FROM ${schema}.nexus_records ORDER BY seq`)).rows;
const versions: RecordVersion[] = rows.map((r) => ({
  elementId: r.element_id, version: Number(r.version), value: r.value, timestamp: r.ts.toISOString(), writtenAt: r.written_at.toISOString(),
  principal: r.principal, prevHash: r.prev_hash, hash: r.hash,
}));
const keys = rows.filter((r) => r.idempotency_key).map((r) => String(r.idempotency_key).split(":")[1]);
const counts = new Map<string, number>();
for (const k of keys) counts.set(k, (counts.get(k) ?? 0) + 1);
const duplicated = [...counts.entries()].filter(([, n]) => n > 1).map(([k]) => k);
const lost = acked.filter((a) => !counts.has(a.key)).map((a) => a.key);
const chain = verifyChain(versions);
const rtos = cycles.map((c) => c.rtoMs).sort((x, y) => x - y);
const result = {
  ranAt: new Date().toISOString(), cycles: CYCLES, leaseTtlMs: TTL,
  writeRtoMs: { max: rtos.at(-1), median: rtos[Math.floor(rtos.length / 2)], all: cycles },
  target: { writeRtoMs: 30_000 },
  acknowledgedWrites: acked.length, ledgerRows: rows.length, duplicated, lost, chain: chain ?? "intact",
  pass: rtos.every((r) => r >= 0 && r <= 30_000) && !duplicated.length && !lost.length && !chain,
};
mkdirSync("docs/level2", { recursive: true });
writeFileSync("docs/level2/failover-drill.json", JSON.stringify(result, null, 2));
console.log(JSON.stringify({ pass: result.pass, writeRtoMs: { max: result.writeRtoMs.max, median: result.writeRtoMs.median }, acknowledgedWrites: acked.length, ledgerRows: rows.length, duplicated: duplicated.length, lost: lost.length, chain: result.chain }));
await admin.query(`DROP SCHEMA ${schema} CASCADE`);
await admin.end();
process.exit(result.pass ? 0 : 1);
