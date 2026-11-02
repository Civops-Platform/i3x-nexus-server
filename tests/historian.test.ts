// History federation against Ignition's SQL historian layout, built synthetically
// in a throwaway schema (no real historian is touched).

import { test } from "node:test";
import assert from "node:assert/strict";
import pg from "pg";
import { PG_URL, freshSchema } from "./cluster-helpers";
import { startNexus } from "./helpers";
import { exampleConfig } from "./helpers";
import { qualityOfIntegrity } from "../src/history/ignition";

const skip = !PG_URL && "no test Postgres";

test("quality codes", () => {
  assert.equal(qualityOfIntegrity(192), "Good");
  assert.equal(qualityOfIntegrity(100), "Uncertain");
  assert.equal(qualityOfIntegrity(0), "Bad");
});

test("federated history: two partitions, a retired tag id, quality mapped, ordered; historian down → error, never empty", { skip }, async () => {
  const db = await freshSchema();
  const c = new pg.Client(db.url);
  await c.connect();
  const ms = (s: string) => Date.parse(s);
  await c.query(`
    CREATE TABLE sqlth_drv (id int primary key, name text, provider text);
    CREATE TABLE sqlth_scinfo (id int primary key, scname text, drvid int);
    CREATE TABLE sqlth_te (id int primary key, tagpath text, scid int, datatype int, querymode int, created bigint, retired bigint);
    CREATE TABLE sqlth_partitions (pname text, drvid int, start_time bigint, end_time bigint, blocksize bigint, flags int);
    CREATE TABLE sqlt_data_1_2026_09 (tagid int, intvalue bigint, floatvalue double precision, stringvalue text, datevalue timestamp, dataintegrity int, t_stamp bigint);
    CREATE TABLE sqlt_data_1_2026_10 (LIKE sqlt_data_1_2026_09);
    INSERT INTO sqlth_drv VALUES (1, 'site-gateway', 'default');
    INSERT INTO sqlth_scinfo VALUES (1, 'default', 1);
    -- The tag was retired and re-created: both ids carry its history.
    INSERT INTO sqlth_te VALUES (10, 'line2/filler/bowl_level', 1, 1, 0, ${ms("2026-01-01T00:00:00Z")}, ${ms("2026-09-20T00:00:00Z")});
    INSERT INTO sqlth_te VALUES (11, 'line2/filler/bowl_level', 1, 1, 0, ${ms("2026-09-20T00:00:00Z")}, null);
    INSERT INTO sqlth_te VALUES (12, 'line2/other', 1, 1, 0, 0, null);
    INSERT INTO sqlth_partitions VALUES ('sqlt_data_1_2026_09', 1, ${ms("2026-09-01T00:00:00Z")}, ${ms("2026-10-01T00:00:00Z")}, 0, 0);
    INSERT INTO sqlth_partitions VALUES ('sqlt_data_1_2026_10', 1, ${ms("2026-10-01T00:00:00Z")}, ${ms("2026-11-01T00:00:00Z")}, 0, 0);
    INSERT INTO sqlt_data_1_2026_09 VALUES (10, null, 41.5, null, null, 192, ${ms("2026-09-10T08:00:00Z")});
    INSERT INTO sqlt_data_1_2026_09 VALUES (11, null, 43.0, null, null, 192, ${ms("2026-09-25T08:00:00Z")});
    INSERT INTO sqlt_data_1_2026_10 VALUES (11, null, 47.25, null, null, 100, ${ms("2026-10-05T08:00:00Z")});
    INSERT INTO sqlt_data_1_2026_10 VALUES (11, null, null, null, null, 0, ${ms("2026-10-06T08:00:00Z")});
    INSERT INTO sqlt_data_1_2026_10 VALUES (12, null, 999, null, null, 192, ${ms("2026-10-05T09:00:00Z")});
  `);
  await c.end();

  process.env.TEST_HISTORIAN_URL = db.url;
  const cfg = exampleConfig();
  cfg.historians = [{ id: "plant-historian", kind: "ignition-sql", urlEnv: "TEST_HISTORIAN_URL", provider: "default" }];
  const tag = cfg.objects.find((o) => o.id === "tag:line2-filler-bowl-level")!;
  tag.bindings!.value.history = { historian: "plant-historian", address: "line2/filler/bowl_level" };
  const n = await startNexus(cfg);
  try {
    const h = await n.call("POST", "/objects/history", { elementIds: ["tag:line2-filler-bowl-level"], startTime: "2026-09-01T00:00:00Z", endTime: "2026-10-31T00:00:00Z" });
    assert.equal(h.status, 200, JSON.stringify(h.body));
    const vals = h.body.results[0].result.values;
    assert.deepEqual(vals.map((v: any) => v.timestamp), ["2026-09-10T08:00:00.000Z", "2026-09-25T08:00:00.000Z", "2026-10-05T08:00:00.000Z"],
      "both partitions, both tag ids, in order; the Bad null point is not a value");
    assert.deepEqual(vals.map((v: any) => v.value), [41.5, 43, 47.25]);
    assert.equal(vals[2].quality, "Uncertain", "dataintegrity 100 → Uncertain");
    assert.ok(!vals.some((v: any) => v.value === 999), "another tag's points are not mixed in");
    const info = (await (await fetch(`${n.base}/info`)).json() as any).result["x-nexus"].history.federated;
    assert.equal(info[0], "plant-historian (ignition-sql, provider default)");

    await db.drop();
    const down = await n.call("POST", "/objects/history", { elementIds: ["tag:line2-filler-bowl-level"], startTime: "2026-09-01T00:00:00Z", endTime: "2026-10-31T00:00:00Z" });
    assert.equal(down.status, 503, "a historian that cannot answer fails the request");
    assert.match(down.body.responseDetail.detail, /plant-historian/);
  } finally {
    await n.stop();
    await db.drop().catch(() => {});
  }
});
