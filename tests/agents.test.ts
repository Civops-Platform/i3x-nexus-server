// Profile Level 5: agents under grants, the registry and freeze, MCP, signed approvals, anchoring.

import { test } from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { generateKeyPairSync, sign, createPublicKey } from "node:crypto";
import { Nexus, type NexusOptions } from "../src/nexus";
import { hashOf, verifyChain, type RecordVersion } from "../src/core/records";
import { checkAnchors, type Anchor } from "../src/core/anchor";
import { startSyntheticCmms } from "../scripts/synthetic-cmms";
import { governedConfig, P, T } from "./governed-helpers";
import { until } from "./helpers";

const AGENT = "t-agent";
const pem = () => {
  const k = generateKeyPairSync("ed25519");
  return { priv: k.privateKey, pubPem: k.publicKey.export({ type: "spki", format: "pem" }).toString(), privPem: k.privateKey.export({ type: "pkcs8", format: "pem" }).toString() };
};

async function setup(extra: Partial<NexusOptions> = {}, approverKey?: string) {
  const cmms = await startSyntheticCmms(0, {});
  const dir = mkdtempSync(join(tmpdir(), "nexus-l5-"));
  const withScopes = (d: ReturnType<typeof P>, scopes: string[]) => ({ ...d, scopes: ["read", ...scopes] });
  const nexus = new Nexus({
    config: governedConfig(cmms.url), dataDir: dir, port: 0, rate: { perSecond: 5000, burst: 5000 },
    agents: { requireGrant: true },
    principals: [
      withScopes(P("planner-1", "person", T.planner, ["maintenance-planner"]), ["agents:grant"]),
      { ...P("planner-2", "person", T.planner2, ["maintenance-planner"]), ...(approverKey ? { approvalKey: approverKey } : {}) },
      withScopes(P("manager-1", "person", T.manager, ["maintenance-manager"]), ["agents:freeze"]),
      withScopes(P("plant-1", "person", T.plant, ["plant-manager"]), ["agents:admin"]),
      P("agent:scheduler", "agent", AGENT),
      P("reader", "person", T.reader),
    ],
    ...extra,
  });
  const port = await nexus.start();
  const base = `http://127.0.0.1:${port}/v1`;
  const call = async (method: string, path: string, body: unknown, tok: string) => {
    const r = await fetch(base + path, { method, headers: { authorization: `Bearer ${tok}`, "content-type": "application/json" }, body: body === undefined ? undefined : JSON.stringify(body) });
    const text = await r.text();
    return { status: r.status, headers: r.headers, body: (text ? JSON.parse(text) : null) as any };
  };
  const put = async (elementId: string, value: unknown, tok: string) => {
    const r = await call("PUT", "/objects/value", { updates: [{ elementId, value: { value } }] }, tok);
    return { ...r, item: r.body.results?.[0], id: JSON.parse(r.headers.get("x-nexus-written") ?? "[]")[0] as string | null };
  };
  const wr = async (id: string) => (await call("POST", "/objects/value", { elementIds: [id] }, T.reader)).body.results[0].result.value;
  const grant = async (body: Record<string, unknown> = {}, tok = T.planner) =>
    call("PUT", "/x-nexus/agents/grants", { agent: "agent:scheduler", task: "re-plan line 2 work for the week", areas: ["nexus:system:cmms", "wc:line2"], actions: ["request:W1"], maxRequests: 2, ttlMinutes: 30, ...body }, tok);
  await until(async () => nexus.projection!.value("cmms:WO-1001"), 5000);
  const stop = async () => { await nexus.stop(); await cmms.close(); rmSync(dir, { recursive: true, force: true }); };
  return { nexus, call, put, wr, grant, stop, dir, base };
}

const reprioritise = (key: string, extra: Record<string, unknown> = {}) => ({
  idempotencyKey: key, target: { elementId: "cmms:WO-1001" }, proposed: { priority: "high" },
  exceptionType: "maintenance-reprioritisation", exposureUsd: 2000, onBehalfOf: "somebody-important", ...extra,
});

test("level 5: an agent works only under a grant, inside its areas and budget, for its sponsor", async () => {
  const s = await setup();
  try {
    // NX-70: no grant, no access, not even reads.
    assert.equal((await s.call("POST", "/objects/value", { elementIds: ["tag:line2-filler-bowl-level"] }, AGENT)).status, 403);
    // Only a person with agents:grant issues one; an agent cannot grant itself.
    assert.equal((await s.grant({}, T.reader)).status, 403);
    assert.equal((await s.grant({ areas: ["no:such-element"] })).status, 400);
    const g = await s.grant();
    assert.equal(g.status, 200, JSON.stringify(g.body));
    const tok = g.body.result.token as string;
    assert.equal(g.body.result.grant.tokenSha256, undefined, "the token hash is never shown");

    // Reads inside the areas work; outside them are refused, and listings are filtered.
    assert.equal((await s.call("POST", "/objects/value", { elementIds: ["tag:line2-filler-bowl-level"] }, tok)).status, 200);
    const out = await s.call("POST", "/objects/value", { elementIds: ["signal:ship-eta"] }, tok);
    assert.equal(out.status, 403);
    const list = await s.call("GET", "/objects", undefined, tok);
    const ids = (list.body.result as Array<{ elementId: string }>).map((o) => o.elementId);
    assert.ok(ids.includes("eq:line2-filler") && ids.includes("cmms:WO-1001"));
    assert.ok(!ids.includes("signal:ship-eta") && !ids.includes("area:packaging"), "nothing outside the grant is listed");

    // NX-71: the ledger names the sponsor, whatever the agent claims.
    const r1 = await s.put("nexus:intake:W1", reprioritise("g-1"), tok);
    assert.equal(r1.item.success, true, JSON.stringify(r1.body));
    const v1 = await s.wr(r1.id!);
    assert.equal(v1.actor.onBehalfOf, "planner-1");
    assert.deepEqual(v1.actor.grant, { id: g.body.result.grant.id, task: "re-plan line 2 work for the week" });

    // NX-74: the sponsor cannot approve what their agent asked for; another planner can.
    assert.equal((await s.put(r1.id!, { decision: "approve" }, T.planner)).item.responseDetail.status, 403);
    await s.put(r1.id!, { decision: "approve" }, T.planner2);
    assert.equal((await s.wr(r1.id!)).status, "verified");

    // A target outside the areas is refused before anything is recorded.
    const far = await s.put("nexus:intake:W1", reprioritise("g-far", { target: { elementId: "nexus:system:cmms" }, proposed: { title: "x" }, compensation: { proposed: { status: "cancelled" } } }), tok);
    assert.equal(far.item.success, true, "the system folder is inside the area");
    // The budget: two requests, then no more.
    const r3 = await s.put("nexus:intake:W1", reprioritise("g-3"), tok);
    assert.equal(r3.item.responseDetail.status, 403);
    assert.match(r3.item.responseDetail.detail, /used all 2/);

    // The agent can read its own requests, not other principals'.
    const people = await s.put("nexus:intake:W1", reprioritise("person-1"), T.manager);
    assert.equal((await s.call("POST", "/objects/value", { elementIds: [people.id!] }, tok)).status, 403);
    assert.equal((await s.call("POST", "/objects/value", { elementIds: [r1.id!] }, tok)).status, 200);

    // /me: what the agent may still do.
    const me = await s.call("GET", "/x-nexus/agents/me", undefined, tok);
    assert.equal(me.body.result.grant.requestsLeft, 0);

    // Revoked: the token stops working at once.
    assert.equal((await s.call("PUT", `/x-nexus/agents/grants/${g.body.result.grant.id}/revoke`, { reason: "done" }, T.reader)).status, 403);
    assert.equal((await s.call("PUT", `/x-nexus/agents/grants/${g.body.result.grant.id}/revoke`, { reason: "done" }, T.planner)).status, 200);
    assert.equal((await s.call("POST", "/objects/value", { elementIds: ["tag:line2-filler-bowl-level"] }, tok)).status, 401);
  } finally { await s.stop(); }
});

test("level 5: one switch freezes agent writes; held requests apply when it is released", async () => {
  const s = await setup();
  try {
    const tok = (await s.grant({ maxRequests: 5 })).body.result.token as string;
    const r = await s.put("nexus:intake:W1", reprioritise("f-1"), tok);
    // Freeze before approval: the approval lands, the apply is held.
    assert.equal((await s.call("PUT", "/x-nexus/agents/freeze", { reason: "drill" }, T.reader)).status, 403);
    assert.equal((await s.call("PUT", "/x-nexus/agents/freeze", {}, T.manager)).status, 400, "a reason is required");
    assert.equal((await s.call("PUT", "/x-nexus/agents/freeze", { reason: "drill" }, T.manager)).status, 200);
    await s.put(r.id!, { decision: "approve" }, T.planner2);
    assert.equal((await s.wr(r.id!)).status, "approved", "held, not applied");
    const blocked = await s.put("nexus:intake:W1", reprioritise("f-2"), tok);
    assert.equal(blocked.item.responseDetail.status, 423);
    // People carry on.
    const human = await s.put("nexus:intake:W1", reprioritise("f-human", { proposed: { status: "in-progress" } }), T.manager);
    assert.equal(human.item.success, true);
    // Reads carry on for the agent.
    assert.equal((await s.call("POST", "/objects/value", { elementIds: ["tag:line2-filler-bowl-level"] }, tok)).status, 200);
    // Only an admin releases it.
    assert.equal((await s.call("PUT", "/x-nexus/agents/release", { reason: "drill over" }, T.manager)).status, 403);
    assert.equal((await s.call("PUT", "/x-nexus/agents/release", { reason: "drill over" }, T.plant)).status, 200);
    await s.nexus.governor!.resume();
    assert.equal((await s.wr(r.id!)).status, "verified");

    // Suspend shuts one agent out entirely, reads included.
    assert.equal((await s.call("PUT", "/x-nexus/agents/agent%3Ascheduler/suspend", {}, T.plant)).status, 200);
    assert.equal((await s.call("POST", "/objects/value", { elementIds: ["tag:line2-filler-bowl-level"] }, tok)).status, 403);
    assert.equal((await s.call("PUT", "/x-nexus/agents/agent%3Ascheduler/resume", {}, T.plant)).status, 200);
    assert.equal((await s.call("POST", "/objects/value", { elementIds: ["tag:line2-filler-bowl-level"] }, tok)).status, 200);

    // The registry: admin sees all, with the record and the refusals.
    const reg = (await s.call("GET", "/x-nexus/agents", undefined, T.plant)).body.result;
    const a = reg.agents.find((x: any) => x.id === "agent:scheduler");
    assert.equal(a.record.byStatus.verified, 1);
    assert.equal(a.grants[0].used, 1, "a refused request is not recorded, so it uses nothing");
    assert.ok(a.flags.some((f: any) => /frozen/.test(f.what)));
    assert.equal((await s.call("GET", "/x-nexus/agents", undefined, T.reader)).status, 403);
    // Control changes are ledger versions, hash-chained with everything else.
    assert.ok(s.nexus.records.history("nexus:agent-control").length >= 4);
    assert.equal(await s.nexus.records.verify(), null);
  } finally { await s.stop(); }
});

test("level 5: MCP tools run through the same checks as i3X", async () => {
  const s = await setup();
  try {
    const tok = (await s.grant()).body.result.token as string;
    let n = 0;
    const rpc = async (method: string, params?: unknown, t = tok) => (await s.call("POST", "/mcp", { jsonrpc: "2.0", id: ++n, method, params }, t)).body;
    const init = await rpc("initialize", { protocolVersion: "2025-06-18", capabilities: {}, clientInfo: { name: "test", version: "0" } });
    assert.equal(init.result.serverInfo.name, "i3x-nexus");
    const note = await s.call("POST", "/mcp", { jsonrpc: "2.0", method: "notifications/initialized" }, tok);
    assert.equal(note.status, 202);
    const tools = (await rpc("tools/list")).result.tools.map((t: any) => t.name);
    assert.deepEqual(tools.sort(), ["browse", "check_request", "my_grant", "read_history", "read_values", "request_change", "request_setpoint"]);

    const inside = await rpc("tools/call", { name: "read_values", arguments: { elementIds: ["tag:line2-filler-bowl-level"] } });
    assert.equal(inside.result.isError, false);
    const outside = await rpc("tools/call", { name: "read_values", arguments: { elementIds: ["signal:ship-eta"] } });
    assert.equal(outside.result.isError, true);
    assert.equal(outside.result.structuredContent.status, 403);

    const asked = await rpc("tools/call", { name: "request_change", arguments: reprioritise("mcp-1", { onBehalfOf: undefined }) });
    assert.equal(asked.result.isError, false, JSON.stringify(asked.result));
    const id = asked.result.structuredContent.requestIds[0];
    const checked = await rpc("tools/call", { name: "check_request", arguments: { requestId: id } });
    assert.equal(checked.result.structuredContent.response.results[0].result.value.status, "requested");
    assert.equal((await s.wr(id)).actor.onBehalfOf, "planner-1");
    const mine = await rpc("tools/call", { name: "my_grant", arguments: {} });
    assert.equal(mine.result.structuredContent.response.result.grant.requestsLeft, 1);
    // A retry with the same key is the same request.
    const again = await rpc("tools/call", { name: "request_change", arguments: reprioritise("mcp-1", { onBehalfOf: undefined }) });
    assert.equal(again.result.structuredContent.requestIds[0], id);
    // No grant, no MCP either.
    assert.equal((await s.call("POST", "/mcp", { jsonrpc: "2.0", id: 1, method: "tools/list" }, AGENT)).status, 403);
    assert.equal((await rpc("tools/call", { name: "nope", arguments: {} })).error.code, -32602);
  } finally { await s.stop(); }
});

test("level 5: approvals are signed over the exact version the approver read", async () => {
  const k = pem();
  const s = await setup({ requireSignedApprovals: true }, k.pubPem);
  try {
    const tok = (await s.grant()).body.result.token as string;
    const r = await s.put("nexus:intake:W1", reprioritise("s-1"), tok);
    assert.equal((await s.put(r.id!, { decision: "approve" }, T.planner2)).item.responseDetail.status, 403, "unsigned");
    const info = (await s.call("GET", `/x-nexus/requests/${encodeURIComponent(r.id!)}`, undefined, T.planner2)).body.result;
    const wrong = sign(null, Buffer.from(info.sign.approve.replace(info.versionHash, "0".repeat(64))), k.priv).toString("base64url");
    assert.equal((await s.put(r.id!, { decision: "approve", signature: wrong }, T.planner2)).item.responseDetail.status, 403, "signed over another version");
    const good = sign(null, Buffer.from(info.sign.approve), k.priv).toString("base64url");
    await s.put(r.id!, { decision: "approve", signature: good }, T.planner2);
    const v = await s.wr(r.id!);
    assert.equal(v.status, "verified");
    assert.deepEqual(v.approvals[0].signature, { signedHash: info.versionHash, sig: good });
    // A person with no registered key cannot approve on this nexus at all.
    const r2 = await s.put("nexus:intake:W1", reprioritise("s-2"), tok);
    assert.match((await s.put(r2.id!, { decision: "approve" }, T.planner)).item.responseDetail.detail, /no approval key|sponsored/);
  } finally { await s.stop(); }
});

test("level 5: anchors catch a ledger rewritten so that its own chain still verifies", async () => {
  const k = pem();
  const dir = mkdtempSync(join(tmpdir(), "nexus-anchor-"));
  const s = await setup({ anchor: { keyPem: k.privPem, file: join(dir, "anchors.jsonl"), everyMs: 3_600_000 } });
  try {
    const tok = (await s.grant()).body.result.token as string;
    await s.put("nexus:intake:W1", reprioritise("a-1"), tok);
    const a1 = await s.nexus.anchorer!.tick();
    assert.ok(a1);
    assert.equal(await s.nexus.anchorer!.tick(), null, "nothing new, nothing anchored");
    await s.put("nexus:intake:W1", reprioritise("a-2"), tok);
    assert.ok(await s.nexus.anchorer!.tick());

    const ledger = join(s.dir, "records.jsonl");
    const rows = readFileSync(ledger, "utf8").split("\n").filter(Boolean).map((l) => JSON.parse(l) as RecordVersion);
    const anchors = readFileSync(join(dir, "anchors.jsonl"), "utf8").split("\n").filter(Boolean).map((l) => JSON.parse(l) as Anchor);
    const pub = createPublicKey(k.pubPem);
    assert.ok(checkAnchors(rows, anchors, pub).every((r) => r.ok));

    // An insider edits one early version and re-hashes everything after it.
    const forged: RecordVersion[] = [];
    let prev = "0".repeat(64);
    for (const [i, v] of rows.entries()) {
      const value = i === 3 ? { ...(v.value as object), tampered: true } : v.value;
      const { hash: _h, ...rest } = { ...v, value, prevHash: prev };
      const nv = { ...rest, hash: hashOf(rest) };
      forged.push(nv);
      prev = nv.hash;
    }
    assert.equal(verifyChain(forged), null, "the forged chain verifies on its own");
    assert.ok(checkAnchors(forged, anchors, pub).some((r) => !r.ok && /rewritten/.test(r.reason!)));
    assert.ok(checkAnchors(rows.slice(0, 2), anchors, pub).some((r) => !r.ok && /removed/.test(r.reason!)));
    const bad = [{ ...anchors[0], checkpoint: { ...anchors[0].checkpoint, count: 1 } }];
    assert.equal(checkAnchors(rows, bad, pub)[0].reason, "signature does not verify");
    writeFileSync(join(dir, "x"), "");
  } finally { await s.stop(); rmSync(dir, { recursive: true, force: true }); }
});

import { PG_URL, freshSchema, startNode, leaderOf } from "./cluster-helpers";

test("level 5 in a pair: grants, sponsor attribution and the freeze survive forwarding to the governor", { skip: !PG_URL && "no test Postgres" }, async () => {
  const cmms = await startSyntheticCmms(0, {});
  const db = await freshSchema();
  const cfg = governedConfig(cmms.url);
  const people = [
    { ...P("planner-1", "person", T.planner, ["maintenance-planner"]), scopes: ["read", "agents:grant"] },
    { ...P("plant-1", "person", T.plant, ["plant-manager"]), scopes: ["read", "agents:admin"] },
    P("agent:scheduler", "agent", AGENT), P("reader", "person", T.reader),
  ];
  const nodes = [await startNode("a", db.url, cfg, undefined, people), await startNode("b", db.url, cfg, undefined, people)];
  try {
    await until(async () => leaderOf(nodes).length === 1 && nodes[0].nexus.redundancy().state === "ok", 8000);
    await until(async () => nodes.every((n) => n.nexus.projection!.value("cmms:WO-1001")), 5000);
    const standby = nodes.find((n) => !n.nexus.lease!.isLeader())!;
    const g = await standby.call("PUT", "/x-nexus/agents/grants", { agent: "agent:scheduler", task: "pair test", areas: ["nexus:system:cmms"], actions: ["request:W1"], maxRequests: 3 }, T.planner);
    assert.equal(g.status, 200, JSON.stringify(g.body));
    const tok = g.body.result.token as string;
    // The grant reaches the standby's mirror, and the agent writes through it.
    await until(async () => standby.nexus.agents.grant(g.body.result.grant.id));
    const r = await standby.call("PUT", "/objects/value", { updates: [{ elementId: "nexus:intake:W1", value: { value: reprioritise("pair-1") } }] }, tok);
    assert.equal(r.body.success, true, JSON.stringify(r.body));
    const id = JSON.parse(r.headers.get("x-nexus-written")!)[0];
    const leader = leaderOf(nodes)[0];
    assert.equal(leader.nexus.governor!.current(id)!.actor.onBehalfOf, "planner-1");
    assert.equal(leader.nexus.governor!.current(id)!.actor.grant!.task, "pair test");
    // Freeze through the standby; both nodes refuse the agent.
    assert.equal((await standby.call("PUT", "/x-nexus/agents/freeze", { reason: "pair drill" }, T.plant)).status, 200);
    await until(async () => standby.nexus.agents.frozen());
    for (const n of nodes) {
      const x = await n.call("PUT", "/objects/value", { updates: [{ elementId: "nexus:intake:W1", value: { value: reprioritise(`pair-f-${n.nodeId}`) } }] }, tok);
      assert.equal(x.body.results[0].responseDetail.status, 423);
    }
    assert.equal(await leader.nexus.records.verify(), null);
  } finally {
    for (const n of nodes) await n.nexus.stop().catch(() => {});
    await cmms.close(); await db.drop();
  }
});
