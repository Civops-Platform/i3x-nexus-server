import { test } from "node:test";
import assert from "node:assert/strict";
import { appendFileSync, readFileSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { startNexus, TOKENS } from "./helpers";
import { RecordStore } from "../src/core/records";

test("W0: an owner publishes a record; it is versioned and its history is the version list", async () => {
  const n = await startNexus();
  try {
    const put = (v: unknown, ts?: string) => n.call("PUT", "/objects/value", { updates: [{ elementId: "decision:line2-speed-trial", value: { value: v, ...(ts ? { timestamp: ts } : {}) } }] }, TOKENS.publisher);
    assert.equal((await put({ statement: "Run line 2 at 420 bpm for two weeks", owner: "role:ops-manager", status: "open" })).body.success, true);
    assert.equal((await put({ status: "holding" })).body.success, true);
    const v = await n.value("decision:line2-speed-trial");
    assert.equal(v.result.value.statement, "Run line 2 at 420 bpm for two weeks", "partial write merges onto the current version");
    assert.equal(v.result.value.status, "holding");
    const h = await n.call("POST", "/objects/history", { elementIds: ["decision:line2-speed-trial"], startTime: "2000-01-01T00:00:00Z", endTime: "2100-01-01T00:00:00Z" });
    assert.equal(h.body.results[0].result.values.length, 2);
  } finally { await n.stop(); }
});

test("W0 refusals: non-owner, unknown field, realtime value, media item", async () => {
  const n = await startNexus();
  try {
    const r = await n.call("PUT", "/objects/value", { updates: [
      { elementId: "decision:line2-speed-trial", value: { value: { status: "open" } } },                    // agent does not own conclusions
      { elementId: "signal:ship-eta", value: { value: { status: "active", order: "SO-1001" } } },            // agent owns signals
      { elementId: "signal:ship-eta", value: { value: { colour: "red" } } },                                // unknown field
      { elementId: "eq:line2-filler", value: { value: { speed: 900 } } },                                   // realtime: W2, phase 4
      { elementId: "no:such", value: { value: 1 } },
    ] }, TOKENS.agent);
    const st = r.body.results.map((x: any) => x.success ? 200 : x.responseDetail.status);
    assert.deepEqual(st, [403, 200, 400, 403, 404]);
    assert.match(r.body.results[3].responseDetail.detail, /tier W2/);
    const reader = await n.call("PUT", "/objects/value", { updates: [{ elementId: "signal:ship-eta", value: { value: { status: "clear" } } }] }, TOKENS.reader);
    assert.equal(reader.body.results[0].responseDetail.status, 403, "read scope alone publishes nothing");
  } finally { await n.stop(); }
});

test("the record ledger is hash-chained: an edit or a removed line is detected", async () => {
  const n = await startNexus();
  const file = join(n.dir, "records.jsonl");
  try {
    for (const s of ["open", "holding", "closed"]) {
      await n.call("PUT", "/objects/value", { updates: [{ elementId: "decision:line2-speed-trial", value: { value: { status: s } } }] }, TOKENS.publisher);
    }
  } finally { await n.nexus.stop(); }
  assert.equal(await new RecordStore(file).verify(), null);
  const lines = readFileSync(file, "utf8").trim().split("\n");
  writeFileSync(file, lines.map((l, i) => (i === 2 ? l.replace('"holding"', '"closed"') : l)).join("\n") + "\n");
  assert.match((await new RecordStore(file).verify())!.reason, /edited/);
  writeFileSync(file, [lines[0], lines[1], lines[3]].join("\n") + "\n");
  assert.match((await new RecordStore(file).verify())!.reason, /removed/);
  appendFileSync(file, "");
});

test("records survive a restart", async () => {
  const n = await startNexus();
  await n.call("PUT", "/objects/value", { updates: [{ elementId: "signal:ship-eta", value: { value: { status: "active", order: "SO-7" } } }] }, TOKENS.publisher);
  await n.nexus.stop();
  const again = new RecordStore(join(n.dir, "records.jsonl"));
  assert.equal((again.current("signal:ship-eta")!.value as any).order, "SO-7");
});
