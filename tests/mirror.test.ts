// Enterprise tier: one nexus mirrors a site nexus over plain i3X (NX-45, NX-48).

import { test } from "node:test";
import assert from "node:assert/strict";
import { startNexus, until, TOKENS } from "./helpers";
import type { SiteConfig } from "../src/model/config";

test("mirror: same ids; uplink loss → Uncertain; reconnect replays P0, records, current, history in order; writes go to the site", async () => {
  const site = await startNexus();
  process.env.TEST_SITE_TOKEN = TOKENS.reader;
  const entCfg: SiteConfig = {
    site: { id: "enterprise", name: "Enterprise view (synthetic)" },
    namespaces: [], types: [], objects: [], sources: [],
    upstreams: [{ id: "riverside", url: site.base, tokenEnv: "TEST_SITE_TOKEN" }],
  };
  // The enterprise principals must know the same callers for forwarded writes to mean anything;
  // the site re-authenticates them itself.
  const ent = await startNexus(entCfg);
  const mirror = ent.nexus.mirrors.get("riverside")!;
  try {
    await until(async () => mirror.connected, 8000);
    // Same element ids, same structure.
    const siteIds = (await site.call("GET", "/objects")).body.result.map((o: any) => o.elementId).sort();
    const entIds = new Set((await ent.call("GET", "/objects")).body.result.map((o: any) => o.elementId));
    for (const id of siteIds) assert.ok(entIds.has(id), `${id} mounted`);
    await until(async () => (await ent.value("eq:line2-filler")).result.quality === "Good", 5000);

    // Cut the uplink.
    mirror.pause();
    await until(async () => (await ent.value("eq:line2-filler")).result.quality === "Uncertain", 5000);
    const stale = (await ent.value("eq:line2-filler")).result;
    assert.notEqual(stale.value, null, "the last value stays visible, marked not live");

    // While it is down, the site has an alarm transition and a record change.
    const tAlarm = new Date().toISOString();
    site.nexus.cache.set("alarm:line2-filler-high-temp/value", "tripped-during-outage", "Good", tAlarm);
    const w = await site.call("PUT", "/objects/value", { updates: [{ elementId: "signal:ship-eta", value: { value: { status: "active", order: "SO-OUTAGE" } } }] }, TOKENS.publisher);
    assert.equal(w.body.success, true);
    await new Promise((r) => setTimeout(r, 1500));

    mirror.resume();
    await until(async () => mirror.connected, 10_000);
    assert.deepEqual(mirror.replayLog.map((p) => p.phase), ["P0", "P2", "P1", "P3"], "replay order per NX-45");
    assert.ok(mirror.replayLog[0].points > 0, "P0 history replayed");

    // The alarm transition that happened during the outage is in the enterprise history.
    const h = await ent.call("POST", "/objects/history", { elementIds: ["alarm:line2-filler-high-temp"], startTime: new Date(Date.parse(tAlarm) - 1000).toISOString(), endTime: new Date().toISOString() });
    assert.ok(h.body.results[0].result.values.some((v: any) => v.value === "tripped-during-outage"), "missed P0 event backfilled");
    assert.equal((await ent.value("signal:ship-eta")).result.value.order, "SO-OUTAGE");
    await until(async () => (await ent.value("eq:line2-filler")).result.quality === "Good", 5000);

    // A write through the enterprise lands at the site, decided by the site with the caller's own token.
    const fw = await ent.call("PUT", "/objects/value", { updates: [{ elementId: "signal:ship-eta", value: { value: { order: "SO-VIA-ENTERPRISE" } } }] }, TOKENS.publisher);
    assert.equal(fw.body.success, true, JSON.stringify(fw.body));
    assert.equal((await site.value("signal:ship-eta")).result.value.order, "SO-VIA-ENTERPRISE");
    const denied = await ent.call("PUT", "/objects/value", { updates: [{ elementId: "signal:ship-eta", value: { value: { order: "X" } } }] }, TOKENS.reader);
    assert.equal(denied.body.results[0].responseDetail.status, 403, "the site refuses a caller it would refuse directly");
  } finally {
    await ent.stop(); await site.stop();
  }
});
