// Check a nexus ledger against its signed anchors (Agent extension §6, NXA-7).
//   npx tsx scripts/verify-anchors.ts <ledger: records.jsonl | postgres-url> <anchors.jsonl> <anchor-public-key.pem>
// Exit 0: every anchor matches. Exit 1: the ledger was rewritten, truncated, or an anchor is forged.
import { readFileSync } from "node:fs";
import { createPublicKey } from "node:crypto";
import pg from "pg";
import { verifyChain, type RecordVersion } from "../src/core/records";
import { checkAnchors, type Anchor } from "../src/core/anchor";

const [ledger, anchorsFile, keyFile] = process.argv.slice(2);
if (!ledger || !anchorsFile || !keyFile) { console.error("usage: verify-anchors.ts <ledger> <anchors.jsonl> <public-key.pem>"); process.exit(2); }
let rows: RecordVersion[];
if (/^postgres(ql)?:/.test(ledger)) {
  const c = new pg.Client(ledger); await c.connect();
  const r = await c.query("SELECT * FROM nexus_records ORDER BY seq");
  rows = r.rows.map((x) => ({ elementId: x.element_id, version: Number(x.version), value: x.value, timestamp: x.ts.toISOString(), writtenAt: x.written_at.toISOString(), principal: x.principal, prevHash: x.prev_hash, hash: x.hash }));
  await c.end();
} else rows = readFileSync(ledger, "utf8").split("\n").filter((l) => l.trim()).map((l) => JSON.parse(l));
const anchors = readFileSync(anchorsFile, "utf8").split("\n").filter((l) => l.trim()).map((l) => JSON.parse(l) as Anchor);
const chain = verifyChain(rows);
const results = checkAnchors(rows, anchors, createPublicKey(readFileSync(keyFile, "utf8")));
console.log(`ledger ${rows.length} versions; chain ${chain ? `BROKEN at ${chain.line}: ${chain.reason}` : "intact"}`);
for (const r of results) console.log(`${r.ok ? "ok  " : "FAIL"} anchor ${r.at} @${r.count}${r.reason ? `: ${r.reason}` : ""}`);
process.exit(chain || results.some((r) => !r.ok) ? 1 : 0);
