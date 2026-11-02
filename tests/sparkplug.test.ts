import { test } from "node:test";
import assert from "node:assert/strict";
import { createServer, type Socket } from "node:net";
import { Aedes } from "aedes";
import mqtt from "mqtt";
import { get } from "sparkplug-payload";
import { startNexus, until } from "./helpers";
import { metricQuality, num } from "../src/drivers/sparkplug";
import type { SiteConfig } from "../src/model/config";

const sp = get("spBv1.0")!;
const freePort = () => new Promise<number>((r) => { const s = createServer().listen(0, () => { const p = (s.address() as any).port; s.close(() => r(p)); }); });

test("quality property and Long conversion", () => {
  assert.equal(metricQuality({ properties: { Quality: { value: 192 } } }), "Good");
  assert.equal(metricQuality({ properties: { Quality: { value: 500 } } }), "Uncertain");
  assert.equal(metricQuality({ properties: { Quality: { value: 0 } } }), "Bad");
  assert.equal(metricQuality({ isNull: true }), "Bad");
  assert.equal(num({ low: 1, high: 1, unsigned: true }), 2 ** 32 + 1);
});

test("Sparkplug B: birth, alias-only data, seq gap → Uncertain, DDEATH and NDEATH → Bad, stale will ignored", async () => {
  const port = await freePort();
  const broker = await Aedes.createBroker();
  const sockets = new Set<Socket>();
  const srv = createServer((s) => { sockets.add(s); s.on("close", () => sockets.delete(s)); broker.handle(s as any); });
  await new Promise<void>((r) => srv.listen(port, r));
  const cfg: SiteConfig = {
    site: { id: "t", name: "sparkplug test" },
    namespaces: [{ uri: "https://example.com/t", name: "t", version: "1.0" }],
    types: [{ id: "Tag", name: "Tag", namespaceUri: "https://example.com/t", kind: "realtime", attributes: [{ name: "value", dataType: "number" }] }],
    objects: [
      { id: "tag:node-temp", name: "Node temp", typeId: "Tag", bindings: { value: { source: "uns", address: "Plant1/Edge1#Temp" } } },
      { id: "tag:speed", name: "Filler speed", typeId: "Tag", bindings: { value: { source: "uns", address: "Plant1/Edge1/Filler#Speed" } } },
    ],
    sources: [{ id: "uns", driver: "sparkplug", endpoint: `mqtt://127.0.0.1:${port}` }],
  };
  const n = await startNexus(cfg);
  const edge = mqtt.connect(`mqtt://127.0.0.1:${port}`);
  const pub = (kind: string, dev: string | null, payload: object) =>
    edge.publish(`spBv1.0/Plant1/${kind}/Edge1${dev ? "/" + dev : ""}`, sp.encodePayload(payload as any) as Buffer);
  const v = async (id: string) => (await n.value(id)).result;
  try {
    await until(async () => n.nexus.driverStatus()[0].connected);
    if (!edge.connected) await new Promise<void>((r) => edge.once("connect", () => r()));
    const t0 = Date.parse("2026-10-30T09:00:00Z");
    pub("NBIRTH", null, { timestamp: t0, seq: 0, metrics: [{ name: "bdSeq", type: "UInt64", value: 7 }, { name: "Temp", alias: 1, type: "Double", value: 61.5, timestamp: t0 }] });
    pub("DBIRTH", "Filler", { timestamp: t0, seq: 1, metrics: [{ name: "Speed", alias: 2, type: "Double", value: 400, timestamp: t0 }] });
    const b = await until(async () => { const r = await v("tag:speed"); return r.value === 400 && r; });
    assert.equal(b.quality, "Good");
    assert.equal(b.timestamp, "2026-10-30T09:00:00.000Z");
    assert.equal((await v("tag:node-temp")).value, 61.5);

    // Alias-only DDATA resolves through the birth's alias table.
    pub("DDATA", "Filler", { timestamp: t0 + 1000, seq: 2, metrics: [{ alias: 2, type: "Double", value: 412, timestamp: t0 + 1000 }] });
    await until(async () => (await v("tag:speed")).value === 412);

    // seq gap (3 is skipped): values may have been missed → Uncertain, last value kept.
    pub("DDATA", "Filler", { timestamp: t0 + 3000, seq: 4, metrics: [{ alias: 2, type: "Double", value: 999, timestamp: t0 + 3000 }] });
    const gap = await until(async () => { const r = await v("tag:speed"); return r.quality === "Uncertain" && r; });
    assert.equal(gap.value, 412, "the gapped message is not applied");

    // Rebirth restores Good.
    pub("NBIRTH", null, { timestamp: t0 + 4000, seq: 0, metrics: [{ name: "bdSeq", type: "UInt64", value: 8 }, { name: "Temp", alias: 1, type: "Double", value: 62, timestamp: t0 + 4000 }] });
    pub("DBIRTH", "Filler", { timestamp: t0 + 4000, seq: 1, metrics: [{ name: "Speed", alias: 2, type: "Double", value: 405, timestamp: t0 + 4000 }] });
    await until(async () => { const r = await v("tag:speed"); return r.value === 405 && r.quality === "Good"; });

    // DDEATH: the device's metrics go Bad; the node's stay Good.
    pub("DDEATH", "Filler", { timestamp: t0 + 5000, seq: 2 });
    await until(async () => (await v("tag:speed")).quality === "Bad");
    assert.equal((await v("tag:node-temp")).quality, "Good");

    // A stale NDEATH (old bdSeq 7) is ignored; the current one (8) kills the node.
    pub("NDEATH", null, { timestamp: t0 + 6000, metrics: [{ name: "bdSeq", type: "UInt64", value: 7 }] });
    await new Promise((r) => setTimeout(r, 300));
    assert.equal((await v("tag:node-temp")).quality, "Good");
    pub("NDEATH", null, { timestamp: t0 + 7000, metrics: [{ name: "bdSeq", type: "UInt64", value: 8 }] });
    await until(async () => (await v("tag:node-temp")).quality === "Bad");

    // Report-by-exception: no staleness without a declared update period.
    pub("NBIRTH", null, { timestamp: t0 + 8000, seq: 0, metrics: [{ name: "bdSeq", type: "UInt64", value: 9 }, { name: "Temp", alias: 1, type: "Double", value: 60, timestamp: t0 + 8000 }] });
    await until(async () => (await v("tag:node-temp")).quality === "Good");
    await new Promise((r) => setTimeout(r, 3500));
    assert.equal((await v("tag:node-temp")).quality, "Good", "a steady RBE value is not stale");
  } finally {
    edge.end(true);
    await n.stop();
    srv.close(); for (const s of sockets) s.destroy(); broker.close();
  }
});
