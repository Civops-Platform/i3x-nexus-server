import { test } from "node:test";
import assert from "node:assert/strict";
import { validateConfig } from "../src/model/config";
import { LastValueCache } from "../src/core/cache";
import { startNexus } from "./helpers";
import { exampleConfig } from "./helpers";

test("config validation reports every problem at once", () => {
  const c = exampleConfig();
  c.objects.push({ id: "eq:broken", name: "x", typeId: "Filler", parentId: "nowhere", bindings: {} });
  c.types.push({ id: "Setpoint", name: "s", namespaceUri: "https://example.com/i3x/plant/1.0", kind: "realtime", write: "W2", attributes: [{ name: "value", dataType: "number" }] });
  c.objects.push({ id: "sp:no-envelope", name: "x", typeId: "Setpoint", bindings: { value: { source: "sim", address: "sine:" } } });
  const p = validateConfig(c);
  assert.ok(p.some((x) => /parent nowhere/.test(x)));
  assert.ok(p.some((x) => /attribute speed has no binding/.test(x)));
  assert.ok(p.some((x) => /W2 attribute value has no write envelope/.test(x)));
  assert.ok(p.some((x) => /only OPC UA has a write path/.test(x)));
});

test("cache: never received → Bad with null on source loss; stale → Uncertain keeping value", () => {
  const c = new LastValueCache();
  c.declare("a/value", "s", 100);
  c.declare("b/value", "s", 100);
  c.set("b/value", 5, "Good", "2026-10-30T00:00:00Z", 0);
  assert.equal(c.sweep(250), 0);
  assert.equal(c.sweep(400), 1);
  assert.deepEqual([c.get("b/value")!.value, c.get("b/value")!.quality], [5, "Uncertain"]);
  c.sourceLost("s", 500);
  assert.deepEqual([c.get("a/value")!.value, c.get("a/value")!.quality], [null, "Bad"]);
  assert.equal(c.get("b/value")!.quality, "Bad");
});

test("no substitution: a source that cannot connect serves Bad, never demo data", async () => {
  const c = exampleConfig();
  c.sources = [{ id: "sim", driver: "mqtt", endpoint: "mqtt://127.0.0.1:1" }];
  const n = await startNexus(c);
  try {
    await new Promise((r) => setTimeout(r, 5500));
    const r = (await n.value("eq:line2-filler")).result;
    assert.equal(r.quality, "Bad");
    assert.equal(r.value, null);
    const all = await n.call("POST", "/objects/value", { elementIds: (await n.call("GET", "/objects")).body.result.map((o: any) => o.elementId) });
    for (const row of all.body.results) {
      const t = n.nexus.fabric.typeOf(row.elementId);
      if (t?.kind === "realtime") assert.notEqual(row.result.quality, "Good", `${row.elementId} must not read Good`);
    }
  } finally { await n.stop(); }
});

test("/info declares the extensions and the phase honestly", async () => {
  const n = await startNexus();
  try {
    const i = (await (await fetch(`${n.base}/info`)).json()) as any;
    assert.equal(i.result.specVersion, "1.0");
    assert.equal(i.result["x-nexus"].writeTiers.W2, "disabled");
    assert.equal(i.result["x-nexus"].level, 1);
    assert.deepEqual(i.result["x-nexus"].writableNamespaces.sort(), ["https://example.com/i3x/conclusions/1.0", "https://example.com/i3x/signals/1.0", "urn:i3x-nexus:1.0"]);
  } finally { await n.stop(); }
});
