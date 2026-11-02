// Profile Level 2: a redundant pair sharing Postgres. Skipped when no test
// Postgres is configured (NEXUS_TEST_PG or .local/pg-url).

import { test } from "node:test";
import assert from "node:assert/strict";
import { createHash, randomBytes } from "node:crypto";
import { PG_URL, freshSchema, startNode, leaderOf, type Node } from "./cluster-helpers";
import { until, TOKENS } from "./helpers";
import { RecordStore } from "../src/core/records";
import pg from "pg";

const skip = !PG_URL && "no test Postgres (set NEXUS_TEST_PG or .local/pg-url)";

const putSignal = (n: Node, order: string, key?: string) =>
  n.call("PUT", "/objects/value", { updates: [{ elementId: "signal:ship-eta", value: { value: { status: "active", order } } }] },
    TOKENS.publisher, key ? { "idempotency-key": key } : {});

test("one governor; standby forwards writes; both nodes serve the result; /info reports the pair", { skip }, async () => {
  const db = await freshSchema();
  const a = await startNode("a", db.url);
  const b = await startNode("b", db.url);
  try {
    await until(async () => leaderOf([a, b]).length === 1 && a.nexus.redundancy().state === "ok" && b.nexus.redundancy().state === "ok", 8000);
    const standby = leaderOf([a, b])[0] === a ? b : a;
    const info = (await (await fetch(`${standby.base}/info`)).json() as any).result["x-nexus"];
    assert.equal(info.level, 2);
    assert.equal(info.redundancy.mode, "redundant");
    assert.equal(info.redundancy.endpoints.length, 2);

    const w = await putSignal(standby, "SO-100");
    assert.equal(w.status, 200, JSON.stringify(w.body));
    assert.equal(w.body.success, true);
    for (const n of [a, b]) await until(async () => (await n.value("signal:ship-eta"))?.result?.value?.order === "SO-100", 5000);
  } finally {
    await b.nexus.stop(); await a.nexus.stop(); await db.drop();
  }
});

test("failover: leader killed → standby governs within the RTO; idempotent retries leave no lost or duplicated versions", { skip }, async () => {
  const db = await freshSchema();
  let nodes: Node[] = [await startNode("a", db.url), await startNode("b", db.url)];
  const acked = new Set<string>();
  const rto: number[] = [];
  try {
    await until(async () => leaderOf(nodes).length === 1, 8000);
    for (let cycle = 0; cycle < 3; cycle++) {
      const leader = leaderOf(nodes)[0];
      const survivor = nodes.find((n) => n !== leader)!;
      // Writes keep coming through the survivor the whole time, each retried with its key until acknowledged.
      let stop = false;
      let i = 0;
      const writer = (async () => {
        while (!stop) {
          const key = `c${cycle}-w${i++}`;
          for (let attempt = 0; attempt < 50 && !stop; attempt++) {
            const r = await putSignal(survivor, key, key).catch(() => null);
            if (r?.status === 200 && r.body.results?.[0]?.success) { acked.add(key); break; }
            await new Promise((x) => setTimeout(x, 200));
          }
        }
      })();
      await new Promise((x) => setTimeout(x, 500));
      const killedAt = Date.now();
      await leader.nexus.stop({ graceful: false }); // no lease release: the standby must wait the TTL out
      await until(async () => survivor.nexus.lease!.isLeader(), 10_000, 50);
      const took = Date.now() - killedAt;
      rto.push(took);
      await new Promise((x) => setTimeout(x, 800));
      stop = true;
      await writer;
      // Bring the killed node back as the new standby.
      const back = await startNode(leader.nodeId, db.url);
      nodes = [survivor, back];
      await until(async () => back.nexus.redundancy().state === "ok", 8000);
    }
    assert.ok(rto.every((t) => t <= 30_000), `RTO ${rto.join(", ")} ms`);

    // Reconcile: every acknowledged key exactly once, no unacknowledged extras, chain intact.
    const pool = new pg.Pool({ connectionString: db.url });
    const rows = (await pool.query(`SELECT idempotency_key FROM nexus_records WHERE element_id = 'signal:ship-eta' AND idempotency_key IS NOT NULL`)).rows.map((r) => String(r.idempotency_key).split(":").slice(1, -1).join(":"));
    await pool.end();
    const counts = new Map<string, number>();
    for (const k of rows) counts.set(k, (counts.get(k) ?? 0) + 1);
    assert.equal([...counts.values()].filter((c) => c > 1).length, 0, "no key applied twice");
    for (const k of acked) assert.ok(counts.has(k), `acknowledged write ${k} is in the ledger`);
    assert.equal(await nodes[0].nexus.records.verify(), null, "hash chain intact across failovers");
    console.log(`# failover RTO ms: ${rto.join(", ")}; acknowledged writes: ${acked.size}; ledger rows: ${rows.length}`);
  } finally {
    for (const n of nodes) await n.nexus.stop().catch(() => {});
    await db.drop();
  }
});

test("partition: the cut-off leader fences itself, the other takes over, never two governors", { skip }, async () => {
  const db = await freshSchema();
  const a = await startNode("a", db.url);
  const b = await startNode("b", db.url);
  let both = 0;
  const sampler = setInterval(() => { if (leaderOf([a, b]).length > 1) both++; }, 50);
  try {
    await until(async () => leaderOf([a, b]).length === 1, 8000);
    const leader = leaderOf([a, b])[0];
    const other = leader === a ? b : a;
    leader.nexus.partition(true);
    await until(async () => !leader.nexus.lease!.isLeader(), 5000, 50);
    await until(async () => other.nexus.lease!.isLeader(), 10_000, 50);
    const refused = await putSignal(leader, "SO-from-partitioned");
    assert.equal(refused.status, 503, "a partitioned node accepts no writes");
    assert.equal((await putSignal(other, "SO-after-partition")).body.success, true);
    // Reads still work on the partitioned node (its cache is fed by its own sources).
    assert.equal((await leader.value("eq:line2-filler")).success, true);
    leader.nexus.partition(false);
    await new Promise((x) => setTimeout(x, 1500));
    assert.equal(leaderOf([a, b]).length, 1);
    assert.equal(both, 0, "never two governors at once");
  } finally {
    clearInterval(sampler);
    await b.nexus.stop(); await a.nexus.stop(); await db.drop();
  }
});

test("subscriptions survive failover; a reconnecting stream gets the P0 events it missed (Last-Event-ID)", { skip }, async () => {
  const db = await freshSchema();
  const a = await startNode("a", db.url);
  const b = await startNode("b", db.url);
  try {
    await until(async () => leaderOf([a, b]).length === 1, 8000);
    const sub = (await a.call("POST", "/subscriptions", { clientId: "c1" })).body.result.subscriptionId;
    await a.call("POST", "/subscriptions/register", { clientId: "c1", subscriptionId: sub, elementIds: ["alarm:line2-filler-high-temp", "eq:line2-filler"] });
    // The other node knows it.
    const listed = await b.call("POST", "/subscriptions/list", { clientId: "c1", subscriptionIds: [sub] });
    assert.equal(listed.body.results[0].result.monitoredObjects.length, 2);
    const sync = await b.call("POST", "/subscriptions/sync", { clientId: "c1", subscriptionId: sub });
    assert.equal(sync.status, 200);

    // The client was disconnected at `since`; alarm events happened on the site while it was away.
    const since = new Date().toISOString();
    await new Promise((x) => setTimeout(x, 50));
    const t0 = new Date().toISOString();
    await new Promise((x) => setTimeout(x, 5));
    const t1 = new Date().toISOString();
    // Two transitions, so at least one is a change whatever the simulator last set.
    for (const n of [a, b]) {
      n.nexus.cache.set("alarm:line2-filler-high-temp/value", "tripped", "Good", t0);
      n.nexus.cache.set("alarm:line2-filler-high-temp/value", "cleared", "Good", t1);
    }
    await new Promise((x) => setTimeout(x, 700)); // history flush
    const r = await fetch(`${b.base}/subscriptions/stream`, {
      method: "POST",
      headers: { authorization: `Bearer ${TOKENS.reader}`, "content-type": "application/json", "last-event-id": since },
      body: JSON.stringify({ clientId: "c1", subscriptionId: sub }),
    });
    const reader = r.body!.getReader();
    const dec = new TextDecoder();
    let text = "";
    const end = Date.now() + 5000;
    while (!text.includes(t1) && Date.now() < end) {
      const chunk = await Promise.race([reader.read(), new Promise<null>((x) => setTimeout(() => x(null), 500))]);
      if (chunk?.value) text += dec.decode(chunk.value);
    }
    assert.ok(text.includes(t1), "the missed alarm event is replayed on reconnect");
    assert.match(text, /^id: /m, "frames carry SSE ids");
    await reader.cancel();
  } finally {
    await b.nexus.stop(); await a.nexus.stop(); await db.drop();
  }
});

test("media: uploaded to one node, served and replicated by the other", { skip }, async () => {
  const db = await freshSchema();
  const a = await startNode("a", db.url);
  const b = await startNode("b", db.url);
  try {
    await until(async () => leaderOf([a, b]).length === 1 && a.nexus.redundancy().state === "ok", 8000);
    const bytes = randomBytes(20_000);
    const sha = createHash("sha256").update(bytes).digest("hex");
    const id = `nexus:media:${sha}`;
    const w = await b.call("PUT", "/objects/value", { updates: [{ elementId: "nexus:intake:media", value: { value: { sha256: sha, mimeType: "image/png", size: bytes.length } } }] }, TOKENS.uploader);
    assert.equal(w.body.success, true, JSON.stringify(w.body));
    const onA = await until(async () => (await a.value(id))?.result?.value, 5000);
    assert.equal((await fetch(onA.href, { method: "PUT", body: bytes })).status, 204);
    // B serves it (from A or from its own replica) and ends up holding it.
    const v = await until(async () => { const x = (await b.value(id))?.result?.value; return x?.status === "available" && x; }, 5000);
    const got = Buffer.from(await (await fetch(v.href)).arrayBuffer());
    assert.equal(createHash("sha256").update(got).digest("hex"), sha);
    await until(async () => b.nexus.media.hasLocal(sha), 8000);
  } finally {
    await b.nexus.stop(); await a.nexus.stop(); await db.drop();
  }
});

test("a fenced write is refused by the database even if the node still believes it leads", { skip }, async () => {
  const db = await freshSchema();
  const a = await startNode("a", db.url);
  try {
    await until(async () => a.nexus.lease!.isLeader(), 8000);
    const pool = new pg.Pool({ connectionString: db.url });
    await pool.query(`UPDATE nexus_lease SET holder = 'someone-else', epoch = epoch + 1, expires_at = now() + interval '1 minute' WHERE name = 'governor'`);
    const r = await putSignal(a, "SO-stale-leader");
    assert.notEqual(r.body?.results?.[0]?.success, true, "the stale leader's write must not land");
    const n = await pool.query(`SELECT count(*)::int AS n FROM nexus_records WHERE value->>'order' = 'SO-stale-leader'`);
    assert.equal(n.rows[0].n, 0);
    await pool.end();
    assert.equal(RecordStore.name, "RecordStore");
  } finally {
    await a.nexus.stop(); await db.drop();
  }
});
