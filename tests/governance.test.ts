// Profile Level 3: governed record writes (W1) against a system of record.

import { test } from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Nexus } from "../src/nexus";
import { startSyntheticCmms } from "../scripts/synthetic-cmms";
import { until } from "./helpers";

import { AUTH_NS, governedConfig, P, T } from "./governed-helpers";
export { governedConfig, P, T };

async function setup(cmmsOpts: Parameters<typeof startSyntheticCmms>[1] = {}, expiryMs?: number) {
  const cmms = await startSyntheticCmms(0, cmmsOpts);
  const dir = mkdtempSync(join(tmpdir(), "nexus-gov-"));
  const cfg = governedConfig(cmms.url);
  const nexus = new Nexus({
    config: cfg, dataDir: dir, port: 0, rate: { perSecond: 5000, burst: 5000 },
    principals: [
      P("planner-1", "person", T.planner, ["maintenance-planner"]),
      P("planner-2", "person", T.planner2, ["maintenance-planner"]),
      P("manager-1", "person", T.manager, ["maintenance-manager"]),
      P("plant-1", "person", T.plant, ["plant-manager"]),
      P("agent:scheduler", "agent", T.agent, ["maintenance-manager"]), // holds the role, still may not approve
      P("authority-register", "service", T.authority, [], [AUTH_NS]),
      P("reader", "person", T.reader),
    ],
  });
  if (expiryMs) (nexus.governor as any).d.expiryMs = expiryMs;
  const port = await nexus.start();
  const base = `http://127.0.0.1:${port}/v1`;
  const call = async (method: string, path: string, body: unknown, tok: string) => {
    const r = await fetch(base + path, { method, headers: { authorization: `Bearer ${tok}`, "content-type": "application/json" }, body: JSON.stringify(body) });
    return { status: r.status, headers: r.headers, body: (await r.json()) as any };
  };
  const put = async (elementId: string, value: unknown, tok: string) => {
    const r = await call("PUT", "/objects/value", { updates: [{ elementId, value: { value } }] }, tok);
    return { ...r, item: r.body.results[0], id: JSON.parse(r.headers.get("x-nexus-written") ?? "[]")[0] as string | null };
  };
  const wr = async (id: string) => (await call("POST", "/objects/value", { elementIds: [id] }, T.reader)).body.results[0].result.value;
  const hist = async (id: string) => (await call("POST", "/objects/history", { elementIds: [id], startTime: "2000-01-01T00:00:00Z", endTime: "2100-01-01T00:00:00Z" }, T.reader)).body.results[0].result.values;
  await until(async () => nexus.projection!.value("cmms:WO-1001"), 5000);
  const request = (body: Record<string, unknown>, tok = T.agent) => put("nexus:intake:W1", body, tok);
  const stop = async () => { await nexus.stop(); await cmms.close(); rmSync(dir, { recursive: true, force: true }); };
  return { nexus, cmms, put, wr, hist, request, stop, call };
}

const reprioritise = (key: string, extra: Record<string, unknown> = {}) => ({
  idempotencyKey: key, target: { elementId: "cmms:WO-1001" }, proposed: { priority: "high" },
  exceptionType: "maintenance-reprioritisation", exposureUsd: 2000,
  justification: { decisionRef: "decision:line2-speed-trial", falsification: { metric: "unplanned-downtime-min", target: "< 30", deadline: "2026-11-30T00:00:00Z" } },
  ...extra,
});

test("W1 happy path: an agent requests, a planner approves, the CMMS changes, read-back verifies; the ledger is the audit trail", async () => {
  const s = await setup();
  try {
    const r = await s.request(reprioritise("k1"));
    assert.equal(r.item.success, true, JSON.stringify(r.body));
    assert.match(r.id!, /^nexus:wr:[0-9a-f]{24}$/);
    let v = await s.wr(r.id!);
    assert.equal(v.status, "requested");
    assert.equal(v.authority.escalation, "stays_local");
    assert.deepEqual(v.authority.approverRoles.sort(), ["maintenance-manager", "maintenance-planner"]);
    assert.equal(v.prior.version, "1");

    // Who may not decide.
    assert.equal((await s.put(r.id!, { decision: "approve" }, T.agent)).item.responseDetail.status, 403, "an agent never approves, whatever roles it holds");
    assert.equal((await s.put(r.id!, { decision: "approve" }, T.reader)).item.responseDetail.status, 403, "no approving role");
    assert.equal((await s.put(r.id!, { status: "verified" }, T.planner)).item.responseDetail.status, 400, "status is computed, never written");

    assert.equal((await s.put(r.id!, { decision: "approve", comment: "fits the window" }, T.planner)).item.success, true);
    v = await s.wr(r.id!);
    assert.equal(v.status, "verified", v.statusDetail);
    assert.equal(s.cmms.orders.get("WO-1001")!.priority, "high");
    assert.equal(v.verification.match, true);
    const statuses = (await s.hist(r.id!)).map((h: any) => h.value.status);
    assert.deepEqual(statuses, ["requested", "approved", "applied", "verified"]);
    assert.equal(await s.nexus.records.verify(), null, "hash chain intact");
    // Retry with the same key: same request, nothing new.
    const again = await s.request(reprioritise("k1"));
    assert.equal(again.id, r.id);
    // Related to its target, its limit and its decision.
    const rel = await s.call("POST", "/objects/related", { elementIds: [r.id] }, T.reader);
    const kinds = rel.body.results[0].result.map((e: any) => `${e.sourceRelationship}:${e.object.elementId}`);
    assert.ok(kinds.includes("Targets:cmms:WO-1001") && kinds.includes("GovernedBy:limit:maintenance-reprioritisation") && kinds.includes("JustifiedBy:decision:line2-speed-trial"), kinds.join(" "));
  } finally { await s.stop(); }
});

test("escalation rule and approver selection", async () => {
  const s = await setup();
  try {
    const cases: Array<[Record<string, unknown>, string, string[] | null, string]> = [
      [{ exposureUsd: 8000 }, "stays_local", ["maintenance-manager"], "above delegated: owner only"],
      [{ exposureUsd: 30000 }, "escalates", ["maintenance-manager"], "at or above materiality"],
      [{ exposureUsd: 100, boundaryTriggers: ["safety"] }, "escalates", ["maintenance-manager"], "a trigger escalates regardless of exposure"],
      [{ exposureUsd: undefined }, "exposure_unknown", ["maintenance-manager"], "no exposure stated"],
      [{ exceptionType: "capex-approval" }, "no_decision_right", null, "no limit for this exception type"],
      [{ reversible: false }, "stays_local", ["plant-manager"], "irreversible: one level above the owner"],
    ];
    for (const [i, [extra, esc, roles, why]] of cases.entries()) {
      const r = await s.request(reprioritise(`e${i}`, extra));
      const v = await s.wr(r.id!);
      assert.equal(v.authority.escalation, esc, why);
      assert.deepEqual(v.authority.approverRoles, roles, why);
      if (!roles) assert.equal(v.status, "rejected", why);
    }
    // A planner cannot approve what only the owner may.
    const big = await s.request(reprioritise("big", { exposureUsd: 8000 }));
    assert.equal((await s.put(big.id!, { decision: "approve" }, T.planner)).item.responseDetail.status, 403);
    assert.equal((await s.put(big.id!, { decision: "approve" }, T.manager)).item.success, true);
    assert.equal((await s.wr(big.id!)).status, "verified");
  } finally { await s.stop(); }
});

test("freshness: a record that moved after the request is not overwritten", async () => {
  const s = await setup();
  try {
    const r = await s.request(reprioritise("stale"));
    const wo = s.cmms.orders.get("WO-1001")!;
    s.cmms.orders.set("WO-1001", { ...wo, priority: "low", version: wo.version + 1 }); // someone changed it in the CMMS
    await s.put(r.id!, { decision: "approve" }, T.planner);
    const v = await s.wr(r.id!);
    assert.equal(v.status, "rejected");
    assert.match(v.statusDetail, /stale/);
    assert.equal(s.cmms.orders.get("WO-1001")!.priority, "low", "the newer value stands");
  } finally { await s.stop(); }
});

test("read-back mismatch: a system that acknowledges but does not apply is caught and compensated", async () => {
  let ignore = false;
  const s = await setup({ ignoreWrites: () => ignore });
  (s.nexus.governor as any).d.verifyWindowMs = 600;
  try {
    const r = await s.request(reprioritise("ghost"));
    ignore = true;
    await s.put(r.id!, { decision: "approve" }, T.planner);
    const v = await s.wr(r.id!);
    assert.equal(v.status, "rolled_back");
    assert.equal(v.verification.match, false);
    assert.match(v.statusDetail, /compensation/);
  } finally { await s.stop(); }
});

test("rollback is a new, linked, approved request; an irreversible request has none", async () => {
  const s = await setup();
  try {
    const r = await s.request(reprioritise("orig"));
    await s.put(r.id!, { decision: "approve" }, T.planner);
    assert.equal(s.cmms.orders.get("WO-1001")!.priority, "high");
    const rb = await s.request({ idempotencyKey: "undo-orig", compensates: r.id }, T.planner2);
    const v = await s.wr(rb.id!);
    assert.deepEqual(v.proposed, { priority: "medium" });
    assert.equal(v.compensates, r.id);
    await s.put(rb.id!, { decision: "approve" }, T.manager);
    assert.equal((await s.wr(rb.id!)).status, "verified");
    assert.equal(s.cmms.orders.get("WO-1001")!.priority, "medium");
    const rel = await s.call("POST", "/objects/related", { elementIds: [r.id] }, T.reader);
    assert.ok(rel.body.results[0].result.some((e: any) => e.sourceRelationship === "CompensatedBy" && e.object.elementId === rb.id));
    // Original stays verified: rollback adds to the record, it erases nothing.
    assert.equal((await s.wr(r.id!)).status, "verified");

    const irr = await s.request(reprioritise("irr", { target: { elementId: "cmms:WO-1002" }, reversible: false }));
    await s.put(irr.id!, { decision: "approve" }, T.plant);
    assert.equal((await s.wr(irr.id!)).status, "verified");
    const undo = await s.request({ idempotencyKey: "undo-irr", compensates: irr.id }, T.planner);
    assert.equal(undo.item.responseDetail.status, 409);
  } finally { await s.stop(); }
});

test("expiry, direct writes, and creating a record", async () => {
  const s = await setup({}, 300);
  try {
    const r = await s.request(reprioritise("slow"));
    await new Promise((x) => setTimeout(x, 400));
    await s.nexus.governor!.resume();
    assert.equal((await s.wr(r.id!)).status, "expired");
    assert.equal(s.cmms.orders.get("WO-1001")!.priority, "medium", "nothing applied because time ran out");

    const direct = await s.put("cmms:WO-1001", { priority: "high" }, T.manager);
    assert.equal(direct.item.responseDetail.status, 403);
    assert.match(direct.item.responseDetail.detail, /nexus:intake:W1/);

    (s.nexus.governor as any).d.expiryMs = 3600_000;
    const create = await s.request({ idempotencyKey: "new-wo", target: { elementId: "nexus:system:cmms" }, proposed: { asset: "Line 2 filler", title: "Check bowl level sensor", priority: "medium" },
      compensation: { proposed: { status: "cancelled" } }, exceptionType: "maintenance-reprioritisation", exposureUsd: 300 });
    assert.equal(create.item.success, true, JSON.stringify(create.body));
    await s.put(create.id!, { decision: "approve" }, T.planner);
    const v = await s.wr(create.id!);
    assert.equal(v.status, "verified", v.statusDetail);
    assert.ok(s.cmms.orders.has(v.target.recordId));
  } finally { await s.stop(); }
});

test("property: whatever the sequence, nothing reaches the CMMS without every check", async () => {
  // A CMMS that logs every mutation; the governor's ledger must account for each one.
  let rnd = 42;
  const rand = () => ((rnd = (rnd * 1103515245 + 12345) % 2 ** 31) / 2 ** 31);
  const pick = <X,>(xs: X[]) => xs[Math.floor(rand() * xs.length)];
  const s = await setup();
  const toks = [T.planner, T.planner2, T.manager, T.plant, T.agent, T.reader];
  const byTok: Record<string, string> = { [T.planner]: "planner-1", [T.planner2]: "planner-2", [T.manager]: "manager-1", [T.plant]: "plant-1", [T.agent]: "agent:scheduler", [T.reader]: "reader" };
  const roles: Record<string, string[]> = { "planner-1": ["maintenance-planner"], "planner-2": ["maintenance-planner"], "manager-1": ["maintenance-manager"], "plant-1": ["plant-manager"], "agent:scheduler": ["maintenance-manager"], reader: [] };
  try {
    const ids: string[] = [];
    for (let i = 0; i < 300; i++) {
      const op = rand();
      if (op < 0.4 || !ids.length) {
        const r = await s.request(reprioritise(`p${i}`, {
          target: { elementId: pick(["cmms:WO-1001", "cmms:WO-1002"]) }, proposed: { priority: pick(["low", "medium", "high", "urgent"]) },
          exposureUsd: pick([100, 4000, 6000, 30000, undefined]), boundaryTriggers: rand() < 0.15 ? ["safety"] : [],
          exceptionType: rand() < 0.1 ? "unknown-type" : "maintenance-reprioritisation", reversible: rand() > 0.1,
        }), pick(toks));
        if (r.id) ids.push(r.id);
      } else if (op < 0.85) {
        await s.put(pick(ids), { decision: rand() < 0.8 ? "approve" : "reject" }, pick(toks));
      } else {
        // Interference: someone edits the CMMS directly.
        const id = pick(["WO-1001", "WO-1002"]);
        const wo = s.cmms.orders.get(id)!;
        s.cmms.orders.set(id, { ...wo, assignedTo: `crew-${i}`, version: wo.version + 1 });
      }
    }
    let applied = 0;
    for (const id of ids) {
      const v = s.nexus.governor!.current(id)!;
      if (!["applied", "verified", "rolled_back"].includes(v.status)) continue;
      applied++;
      const approval = v.approvals.find((a) => a.decision === "approve");
      assert.ok(approval, `${id} applied without an approval`);
      assert.notEqual(approval!.principal, v.actor.principal, `${id} approved by its requester`);
      assert.notEqual(approval!.principal, "agent:scheduler", `${id} approved by an agent`);
      assert.ok(v.authority.approverRoles?.includes(approval!.role) && roles[approval!.principal].includes(approval!.role), `${id} approved without the role`);
      // Freshness: the version applied against is the version requested against.
      const versions = s.nexus.records.history(id).map((h) => (h.value as any).status);
      assert.deepEqual(versions.slice(0, 3), ["requested", "approved", "applied"], `${id} skipped a state: ${versions.join(">")}`);
    }
    assert.ok(applied > 3, `the run exercised applies (${applied})`);
    assert.equal(await s.nexus.records.verify(), null);
    console.log(`# property run: ${ids.length} requests, ${applied} applied, all with valid approvals`);
  } finally { await s.stop(); }
});

import { PG_URL, freshSchema, startNode, leaderOf } from "./cluster-helpers";

test("cluster: an approved request whose governor dies is applied exactly once by the next governor", { skip: !PG_URL && "no test Postgres" }, async () => {
  let down = false;
  const cmms = await startSyntheticCmms(0, { failNext: () => down });
  const db = await freshSchema();
  const cfg = governedConfig(cmms.url);
  const people = [P("planner-1", "person", T.planner, ["maintenance-planner"]), P("agent:scheduler", "agent", T.agent), P("reader", "person", T.reader)];
  const a = await startNode("a", db.url, cfg, undefined, people);
  const b = await startNode("b", db.url, cfg, undefined, people);
  let nodes = [a, b];
  try {
    await until(async () => leaderOf(nodes).length === 1 && a.nexus.redundancy().state === "ok", 8000);
    await until(async () => a.nexus.projection!.value("cmms:WO-1001") && b.nexus.projection!.value("cmms:WO-1001"), 5000);
    const leader = leaderOf(nodes)[0];
    const standby = nodes.find((n) => n !== leader)!;
    // Request and approve through the STANDBY: both are forwarded to the governor.
    const req = await standby.call("PUT", "/objects/value", { updates: [{ elementId: "nexus:intake:W1", value: { value: reprioritise("ha-1") } }] }, T.agent);
    assert.equal(req.body.success, true, JSON.stringify(req.body));
    const id = JSON.parse(req.headers.get("x-nexus-written")!)[0];
    down = true; // the CMMS refuses writes: the approval lands, the apply cannot
    const appr = await standby.call("PUT", "/objects/value", { updates: [{ elementId: id, value: { value: { decision: "approve" } } }] }, T.planner);
    assert.equal(appr.body.success, true, JSON.stringify(appr.body));
    await until(async () => standby.nexus.governor!.current(id)?.status === "approved", 5000);
    await leader.nexus.stop({ graceful: false });
    nodes = [standby];
    down = false;
    await until(async () => standby.nexus.lease!.isLeader(), 10_000, 50);
    await until(async () => standby.nexus.governor!.current(id)?.status === "verified", 15_000);
    assert.equal(cmms.orders.get("WO-1001")!.priority, "high");
    assert.equal(cmms.orders.get("WO-1001")!.version, 2, "applied exactly once");
    assert.equal(await standby.nexus.records.verify(), null);
  } finally {
    for (const n of nodes) await n.nexus.stop().catch(() => {});
    await cmms.close(); await db.drop();
  }
});
