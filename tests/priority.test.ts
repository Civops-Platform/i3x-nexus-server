import { test } from "node:test";
import assert from "node:assert/strict";
import { startNexus, until, TOKENS } from "./helpers";
import { Lanes, classify } from "../src/core/priority";

test("classify: the server assigns the lane from what is asked, not from the caller", () => {
  assert.equal(classify("POST", "/objects/history", ["P0"]), "P3");
  assert.equal(classify("POST", "/objects/value", ["P0", "P0"]), "P0");
  assert.equal(classify("POST", "/objects/value", ["P0", "P1"]), "P1");
  assert.equal(classify("PUT", "/objects/value", ["P2"]), "P2");
  assert.equal(classify("GET", "/x-nexus/media/abc", []), "P3");
});

test("lanes: a full P3 lane refuses; P0 is admitted even when every other lane is full", async () => {
  const lanes = new Lanes({ P0: { concurrency: 1, queue: 1 }, P1: { concurrency: 1, queue: 0 }, P2: { concurrency: 1, queue: 0 }, P3: { concurrency: 1, queue: 0 } });
  try {
    const a = await lanes.admit("P3");
    assert.ok(a.ok);
    const b = await lanes.admit("P3");
    assert.equal(b.ok, false);
    const p0a = await lanes.admit("P0");
    assert.ok(p0a.ok);
    // P0 past its concurrency queues instead of failing, and runs when a P0 slot frees.
    const pending = lanes.admit("P0");
    if (p0a.ok) p0a.release();
    assert.ok((await pending).ok);
  } finally { lanes.close(); }
});

test("overload: history (P3) is shed with 503 + Retry-After while P0 alarm reads and P1 reads still answer", async () => {
  let lag = 0;
  const n = await startNexus(undefined, { lagOverride: () => lag });
  try {
    await until(async () => (await n.value("alarm:line2-filler-high-temp")).result.quality === "Good");
    lag = 300; // between the P2 (250 ms) and P1 (1000 ms) shed points
    const h = await n.call("POST", "/objects/history", { elementIds: ["tag:line2-filler-bowl-level"], startTime: "2000-01-01T00:00:00Z", endTime: "2100-01-01T00:00:00Z" });
    assert.equal(h.status, 503);
    assert.ok(Number(h.headers.get("retry-after")) > 0);
    const w = await n.call("PUT", "/objects/value", { updates: [{ elementId: "signal:ship-eta", value: { value: { status: "clear" } } }] }, TOKENS.publisher);
    assert.equal(w.status, 429, "P2 is refused at this load");
    assert.equal((await n.value("eq:line2-filler")).success, true, "P1 still answers");
    lag = 5000;
    assert.equal((await n.call("POST", "/objects/value", { elementIds: ["eq:line2-filler"] })).status, 429, "P1 refused at extreme load");
    assert.equal((await n.value("alarm:line2-filler-high-temp")).success, true, "P0 is never shed");
    assert.equal((await fetch(`${n.base}/info`)).status, 200, "/info always answers");
  } finally { await n.stop(); }
});

test("stream: a P0 alarm is delivered at once, ahead of the P1 tick", async () => {
  const n = await startNexus(undefined, { streamTickMs: 60_000 });
  try {
    const sub = (await n.call("POST", "/subscriptions", { clientId: "c1" })).body.result.subscriptionId;
    await n.call("POST", "/subscriptions/register", { clientId: "c1", subscriptionId: sub, elementIds: ["alarm:line2-filler-high-temp", "eq:line2-filler"] });
    const ac = new AbortController();
    const r = await fetch(`${n.base}/subscriptions/stream`, { method: "POST", signal: ac.signal, headers: { authorization: `Bearer ${TOKENS.reader}`, "content-type": "application/json" }, body: JSON.stringify({ clientId: "c1", subscriptionId: sub }) });
    const reader = r.body!.getReader();
    const dec = new TextDecoder();
    let text = "";
    // Drain the opening frame, then force an alarm change.
    text += dec.decode((await reader.read()).value);
    const t0 = Date.now();
    n.nexus.cache.set("alarm:line2-filler-high-temp/value", true, "Good", new Date().toISOString());
    n.nexus.cache.set("alarm:line2-filler-high-temp/value", false, "Good", new Date().toISOString());
    let got = "";
    while (!got.includes("alarm:line2-filler-high-temp")) got += dec.decode((await reader.read()).value);
    assert.ok(Date.now() - t0 < 1000, "delivered without waiting for the 60 s tick");
    ac.abort();
  } finally { await n.stop(); }
});
