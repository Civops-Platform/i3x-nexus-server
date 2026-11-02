// Drivers against real servers: an in-process OPC UA server (node-opcua) and an
// in-process MQTT broker (aedes). Each test also kills its source, because the
// thing this nexus must never do is keep serving a lost source's values as Good.

import { test } from "node:test";
import assert from "node:assert/strict";
import { createServer } from "node:net";
import { hostname } from "node:os";
import { OPCUAServer } from "node-opcua-server";
import { Variant, DataType, StatusCodes, DataValue } from "node-opcua-client";
import { Aedes } from "aedes";
import mqtt from "mqtt";
import { startNexus, until } from "./helpers";
import type { SiteConfig } from "../src/model/config";

const freePort = () => new Promise<number>((r) => { const s = createServer().listen(0, () => { const p = (s.address() as any).port; s.close(() => r(p)); }); });

function siteWith(source: SiteConfig["sources"][number], address: string): SiteConfig {
  return {
    site: { id: "t", name: "driver test" },
    namespaces: [{ uri: "https://example.com/t", name: "t", version: "1.0" }],
    types: [{ id: "AnalogTag", name: "Analog tag", namespaceUri: "https://example.com/t", kind: "realtime", attributes: [{ name: "value", dataType: "number" }] }],
    objects: [{ id: "tag:speed", name: "Speed", typeId: "AnalogTag", bindings: { value: { source: source.id, address, scanMs: 200 } } }],
    sources: [source],
  };
}

test("OPC UA: values, source timestamps, status → quality, and session loss → Bad", async () => {
  const port = await freePort();
  const server = new OPCUAServer({ port, resourcePath: "/UA/test", buildInfo: { productName: "test" } });
  await server.initialize();
  const ns = server.engine.addressSpace!.getOwnNamespace();
  const dev = ns.addObject({ organizedBy: server.engine.addressSpace!.rootFolder.objects, browseName: "Line2" });
  let speed = 400;
  let status = StatusCodes.Good;
  const v = ns.addVariable({ componentOf: dev, browseName: "Speed", nodeId: "s=Line2.Speed", dataType: "Double",
    value: { timestamped_get: () => new DataValue({ value: new Variant({ dataType: DataType.Double, value: speed }), statusCode: status, sourceTimestamp: new Date("2026-10-30T10:00:00Z"), serverTimestamp: new Date() }) } });
  void v;
  await server.start();

  const n = await startNexus(siteWith({ id: "ua", driver: "opcua", endpoint: `opc.tcp://${hostname()}:${port}/UA/test` }, "ns=1;s=Line2.Speed"));
  try {
    const first = await until(async () => { const r = (await n.value("tag:speed")).result; return r.value === 400 && r; });
    assert.equal(first.quality, "Good");
    assert.equal(first.timestamp, "2026-10-30T10:00:00.000Z", "the source timestamp is kept, not replaced by receive time");
    speed = 412.5;
    await until(async () => (await n.value("tag:speed")).result.value === 412.5);
    status = StatusCodes.UncertainLastUsableValue;
    speed = 413;
    await until(async () => (await n.value("tag:speed")).result.quality === "Uncertain");

    await server.shutdown(0);
    const lost = await until(async () => { const r = (await n.value("tag:speed")).result; return r.quality === "Bad" && r; }, 20_000);
    assert.equal(lost.value, null, "a Bad scalar is served without a value — never a fresh-looking number");
    assert.ok(n.nexus.driverStatus()[0].connected === false);
  } finally { await n.stop(); await server.shutdown(0).catch(() => {}); }
});

test("MQTT: JSON and bare payloads, staleness → Uncertain, broker loss → Bad", async () => {
  const port = await freePort();
  const broker = await Aedes.createBroker();
  const sockets = new Set<import("node:net").Socket>();
  const srv = createServer((sock) => { sockets.add(sock); sock.on("close", () => sockets.delete(sock)); broker.handle(sock as any); });
  await new Promise<void>((r) => srv.listen(port, r));

  const cfg = siteWith({ id: "uns", driver: "mqtt", endpoint: `mqtt://127.0.0.1:${port}` }, "site/line2/speed#value");
  cfg.objects.push({ id: "tag:bare", name: "Bare", typeId: "AnalogTag", bindings: { value: { source: "uns", address: "site/line2/bare", scanMs: 200 } } });
  const n = await startNexus(cfg);
  const pub = mqtt.connect(`mqtt://127.0.0.1:${port}`);
  try {
    await until(async () => n.nexus.driverStatus()[0].connected);
    if (!pub.connected) await new Promise<void>((r) => pub.once("connect", () => r()));
    pub.publish("site/line2/speed", JSON.stringify({ value: 401, timestamp: "2026-10-30T11:00:00Z" }));
    pub.publish("site/line2/bare", "17");
    const s = await until(async () => { const r = (await n.value("tag:speed")).result; return r.value === 401 && r; });
    assert.equal(s.quality, "Good");
    assert.equal(s.timestamp, "2026-10-30T11:00:00.000Z");
    const b = await until(async () => { const r = (await n.value("tag:bare")).result; return r.value === 17 && r; });
    assert.equal(b.quality, "Uncertain", "a payload with no timestamp is not a Good reading");

    // No new value for > 3× scan: Uncertain, last value kept.
    const stale = await until(async () => { const r = (await n.value("tag:speed")).result; return r.quality === "Uncertain" && r; }, 5000);
    assert.equal(stale.value, 401);

    pub.end(true);
    // Kill the broker: close the listener and drop every live connection.
    srv.close();
    for (const sock of sockets) sock.destroy();
    broker.close();
    await until(async () => (await n.value("tag:speed")).result.quality === "Bad", 10_000);
  } finally { await n.stop(); pub.end(true); srv.close(); for (const sock of sockets) sock.destroy(); }
});
