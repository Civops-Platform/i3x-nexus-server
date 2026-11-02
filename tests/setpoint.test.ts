// Profile Level 4: governed setpoint writes (W2) against a real OPC UA server.

import { test } from "node:test";
import assert from "node:assert/strict";
import { createServer } from "node:net";
import { hostname, tmpdir } from "node:os";
import { mkdtempSync, rmSync } from "node:fs";
import { join } from "node:path";
import { OPCUAServer } from "node-opcua-server";
import { Variant, DataType, StatusCodes } from "node-opcua-client";
import { Nexus } from "../src/nexus";
import { sha256, type PrincipalDef } from "../src/core/auth";
import type { SiteConfig } from "../src/model/config";
import { envelopeViolation } from "../src/governance/setpoint";
import { until } from "./helpers";

const freePort = () => new Promise<number>((r) => { const s = createServer().listen(0, () => { const p = (s.address() as any).port; s.close(() => r(p)); }); });
const T = { operator: "t-op", plant: "t-plant", agent: "t-agent", service: "t-svc", reader: "t-read" };
const P = (id: string, kind: PrincipalDef["kind"], tok: string, roles: string[] = []): PrincipalDef => ({ id, kind, scopes: ["read"], owns: [], roles, tokenSha256: sha256(tok) });

async function setup() {
  const port = await freePort();
  const server = new OPCUAServer({ port, resourcePath: "/UA/sp" });
  await server.initialize();
  const ns = server.engine.addressSpace!.getOwnNamespace();
  const folder = ns.addObject({ organizedBy: server.engine.addressSpace!.rootFolder.objects, browseName: "Line2" });
  const plant = { temp: 62, ghost: 50, locked: 10 };
  const mk = (name: keyof typeof plant, writable: boolean, applies: boolean) => ns.addVariable({
    componentOf: folder, browseName: name, nodeId: `s=${name}`, dataType: "Double", minimumSamplingInterval: 100,
    accessLevel: writable ? "CurrentRead | CurrentWrite" : "CurrentRead", userAccessLevel: writable ? "CurrentRead | CurrentWrite" : "CurrentRead",
    value: {
      get: () => new Variant({ dataType: DataType.Double, value: plant[name] }),
      set: (v: Variant) => { if (applies) plant[name] = v.value; return StatusCodes.Good; },
    },
  });
  mk("temp", true, true); mk("ghost", true, false); mk("locked", false, true);
  await server.start();

  const AUTH = "https://example.com/i3x/authority/1.0", PLANT = "https://example.com/i3x/plant/1.0";
  const env = (e: object) => e;
  const cfg: SiteConfig = {
    site: { id: "example-riverside", name: "setpoint test" },
    namespaces: [{ uri: PLANT, name: "plant", version: "1.0" }, { uri: AUTH, name: "authority", version: "1.0" }],
    types: [
      { id: "Setpoint", name: "Setpoint", namespaceUri: PLANT, kind: "realtime", write: "W2", priority: "P1", attributes: [{ name: "value", dataType: "number" }] },
      { id: "AuthorityLimit", name: "Authority limit", namespaceUri: AUTH, kind: "record", write: "W0", attributes: [
        { name: "site", dataType: "string" }, { name: "exceptionType", dataType: "string" }, { name: "ownerRole", dataType: "string" }, { name: "aboveOwnerRole", dataType: "string" },
        { name: "approverRoles", dataType: "array" }, { name: "boundaryTriggers", dataType: "array" }, { name: "delegatedLimitUsd", dataType: "number" }, { name: "materialityThresholdUsd", dataType: "number" }] },
    ],
    objects: [
      { id: "sp:temp", name: "Filler bowl temperature SP", typeId: "Setpoint", bindings: { value: { source: "ua", address: "ns=1;s=temp", envelope: env({ min: 55, max: 70, maxStep: 3 }) } } },
      { id: "sp:ghost", name: "SP that acknowledges and ignores", typeId: "Setpoint", bindings: { value: { source: "ua", address: "ns=1;s=ghost", envelope: env({ min: 0, max: 100, maxStep: 50 }) } } },
      { id: "sp:locked", name: "SP the device will not write", typeId: "Setpoint", bindings: { value: { source: "ua", address: "ns=1;s=locked", envelope: env({ min: 0, max: 100, maxStep: 50 }) } } },
      { id: "limit:setpoint", name: "Setpoint changes", typeId: "AuthorityLimit", initial: { site: "example-riverside", exceptionType: "setpoint-change",
        ownerRole: "shift-supervisor", aboveOwnerRole: "plant-manager", approverRoles: ["shift-supervisor"], delegatedLimitUsd: 1e9, materialityThresholdUsd: 1e9, boundaryTriggers: [] } },
    ],
    sources: [{ id: "ua", driver: "opcua", endpoint: `opc.tcp://${hostname()}:${port}/UA/sp` }],
  };
  const dir = mkdtempSync(join(tmpdir(), "nexus-sp-"));
  const nexus = new Nexus({ config: cfg, dataDir: dir, port: 0, rate: { perSecond: 5000, burst: 5000 }, principals: [
    P("operator-1", "person", T.operator, ["shift-supervisor"]), P("plant-1", "person", T.plant, ["plant-manager"]),
    P("agent:optimizer", "agent", T.agent), P("svc:scheduler", "service", T.service, ["shift-supervisor"]), P("reader", "person", T.reader),
  ] });
  (nexus.governor as any).d.verifyWindowMs = 2_000;
  const hport = await nexus.start();
  const base = `http://127.0.0.1:${hport}/v1`;
  const put = async (elementId: string, value: unknown, tok: string) => {
    const r = await fetch(base + "/objects/value", { method: "PUT", headers: { authorization: `Bearer ${tok}`, "content-type": "application/json" }, body: JSON.stringify({ updates: [{ elementId, value: { value } }] }) });
    const b = (await r.json()) as any;
    return { item: b.results?.[0] ?? b, id: JSON.parse(r.headers.get("x-nexus-written") ?? "[]")[0] as string | null };
  };
  const req = (elementId: string, v: unknown, key: string, tok = T.agent) => put("nexus:intake:W2", { idempotencyKey: key, target: { elementId }, proposed: { value: v }, exceptionType: "setpoint-change" }, tok);
  const wr = (id: string) => nexus.governor!.current(id)!;
  await until(async () => nexus.cache.get("sp:temp/value")?.quality === "Good" && nexus.cache.get("sp:locked/value")?.quality === "Good", 15_000);
  const stop = async () => { await nexus.stop(); await server.shutdown(0); rmSync(dir, { recursive: true, force: true }); };
  return { nexus, plant, put, req, wr, stop };
}

test("envelope check is exact: range, step from the current value, allowed states", () => {
  const e = { min: 55, max: 70, maxStep: 3 };
  assert.equal(envelopeViolation(e, 64, 62), null);
  assert.match(envelopeViolation(e, 71, 69)!, /maximum/);
  assert.match(envelopeViolation(e, 66, 62)!, /step/);
  assert.match(envelopeViolation(e, "64" as never, 62)!, /finite number/);
  assert.match(envelopeViolation(e, NaN, 62)!, /finite/);
  assert.equal(envelopeViolation({ allowed: ["Run", "Hold"] }, "Hold", "Run"), null);
  assert.match(envelopeViolation({ allowed: ["Run", "Hold"] }, "Stop", "Run")!, /one of/);
});

test("W2: a person approves, the OPC UA write lands, read-back verifies; agents and services never approve", { timeout: 60_000 }, async () => {
  const s = await setup();
  try {
    const r = await s.req("sp:temp", 64.5, "k1");
    assert.equal(r.item.success, true, JSON.stringify(r.item));
    assert.equal(s.wr(r.id!).tier, "W2");
    assert.equal(Date.parse(s.wr(r.id!).expiresAt) - Date.now() <= 15 * 60_000 + 1000, true, "W2 expires in 15 minutes");
    assert.equal((await s.put(r.id!, { decision: "approve" }, T.agent)).item.responseDetail.status, 403);
    assert.match((await s.put(r.id!, { decision: "approve" }, T.service)).item.responseDetail.detail, /named person/);
    assert.equal((await s.put(r.id!, { decision: "approve" }, T.operator)).item.success, true);
    const v = s.wr(r.id!);
    assert.equal(v.status, "verified", v.statusDetail ?? "");
    assert.equal(s.plant.temp, 64.5);
    assert.deepEqual(s.nexus.records.history(r.id!).map((h) => (h.value as any).status), ["requested", "approved", "applied", "verified"]);
    // Direct writes are refused, pointing at the intake.
    const direct = await s.put("sp:temp", 65, T.operator);
    assert.match(direct.item.responseDetail.detail, /nexus:intake:W2/);
  } finally { await s.stop(); }
});

test("W2 fuzz: of 10,000 proposed values around the envelope, none outside it becomes a request", { timeout: 60_000 }, async () => {
  const s = await setup();
  try {
    const gov = s.nexus.governor!;
    const p = { id: "agent:optimizer", kind: "agent" as const, scopes: ["read"], owns: [] };
    let accepted = 0, outside = 0;
    let seed = 7;
    const rnd = () => ((seed = (seed * 1103515245 + 12345) % 2 ** 31) / 2 ** 31);
    for (let i = 0; i < 10_000; i++) {
      const kind = rnd();
      const v = kind < 0.05 ? (["64", null, true, NaN, Infinity][i % 5] as unknown) : 50 + rnd() * 25; // 50..75 around 55..70, step 3 from 62
      const r = await gov.request(p, { idempotencyKey: `f${i}`, target: { elementId: "sp:temp" }, proposed: { value: v }, exceptionType: "setpoint-change" }, "W2");
      const inside = typeof v === "number" && Number.isFinite(v) && v >= 55 && v <= 70 && Math.abs(v - 62) <= 3;
      if (r.ok) { accepted++; assert.ok(inside, `accepted ${v}`); } else if (!inside) outside++;
      else assert.fail(`refused an in-envelope value ${v}: ${r.detail}`);
    }
    console.log(`# fuzz: ${accepted} accepted, ${outside} outside refused, 0 outside accepted`);
    assert.ok(accepted > 500 && outside > 5000);
  } finally { await s.stop(); }
});

test("W2 failure modes: ack without apply → compensated; device refusal → rejected; stale → rejected; suspended while degraded", { timeout: 90_000 }, async () => {
  const s = await setup();
  try {
    // A device that acknowledges and does not apply: read-back mismatch, compensation, rolled_back.
    const g = await s.req("sp:ghost", 60, "g1");
    await s.put(g.id!, { decision: "approve" }, T.operator);
    assert.equal(s.wr(g.id!).status, "rolled_back");
    assert.equal(s.wr(g.id!).verification!.match, false);
    assert.equal(s.plant.ghost, 50);

    // The device refuses the write (not writable): a refusal, so rejected, not retried.
    const l = await s.req("sp:locked", 20, "l1");
    await s.put(l.id!, { decision: "approve" }, T.operator);
    assert.equal(s.wr(l.id!).status, "rejected");
    assert.match(s.wr(l.id!).statusDetail!, /refused/);
    assert.equal(s.plant.locked, 10);

    // The value moves between request and approval: stale.
    const st = await s.req("sp:temp", 63, "st1");
    s.plant.temp = 61;
    await until(async () => s.nexus.cache.get("sp:temp/value")?.value === 61, 5000);
    await s.put(st.id!, { decision: "approve" }, T.operator);
    assert.equal(s.wr(st.id!).status, "rejected");
    assert.match(s.wr(st.id!).statusDetail!, /stale/);
    assert.equal(s.plant.temp, 61);

    // Degraded redundancy: new W2 requests refused; an approved one is held, then applied when it clears.
    const held = await s.req("sp:temp", 62.5, "h1");
    let degraded = true;
    (s.nexus as any).w2Suspended = () => (degraded ? "redundancy is degraded (test)" : null);
    assert.equal((await s.req("sp:temp", 62, "h2")).item.responseDetail.status, 503);
    await s.put(held.id!, { decision: "approve" }, T.operator);
    assert.equal(s.wr(held.id!).status, "approved", "held, not applied, while degraded");
    assert.equal(s.plant.temp, 61);
    degraded = false;
    await s.nexus.governor!.resume();
    assert.equal(s.wr(held.id!).status, "verified");
    assert.equal(s.plant.temp, 62.5);
  } finally { await s.stop(); }
});
