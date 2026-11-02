// The Ignition historian adapter on MySQL and SQL Server (Postgres is in historian.test.ts).
// Needs NEXUS_TEST_MYSQL / NEXUS_TEST_MSSQL (or .local/dialects.env); each is skipped without one.

import { test } from "node:test";
import assert from "node:assert/strict";
import { existsSync, readFileSync } from "node:fs";
import mysql from "mysql2/promise";
import mssql from "mssql";
import { IgnitionSqlHistorian } from "../src/history/ignition";

const env: Record<string, string> = {};
if (existsSync(".local/dialects.env")) for (const l of readFileSync(".local/dialects.env", "utf8").split("\n")) { const i = l.indexOf("="); if (i > 0) env[l.slice(0, i)] = l.slice(i + 1); }
const MYSQL = process.env.NEXUS_TEST_MYSQL ?? env.NEXUS_TEST_MYSQL;
const MSSQL = process.env.NEXUS_TEST_MSSQL ?? env.NEXUS_TEST_MSSQL;

const ms = (s: string) => Date.parse(s);
const seed = (t: { bigint: string; float: string; text: string; date: string }) => [
  `CREATE TABLE sqlth_drv (id int primary key, name ${t.text}, provider ${t.text})`,
  `CREATE TABLE sqlth_scinfo (id int primary key, scname ${t.text}, drvid int)`,
  `CREATE TABLE sqlth_te (id int primary key, tagpath ${t.text}, scid int, datatype int, querymode int, created ${t.bigint}, retired ${t.bigint})`,
  `CREATE TABLE sqlth_partitions (pname ${t.text}, drvid int, start_time ${t.bigint}, end_time ${t.bigint}, blocksize ${t.bigint}, flags int)`,
  `CREATE TABLE sqlt_data_1_2026_09 (tagid int, intvalue ${t.bigint}, floatvalue ${t.float}, stringvalue ${t.text}, datevalue ${t.date}, dataintegrity int, t_stamp ${t.bigint})`,
  `CREATE TABLE sqlt_data_1_2026_10 (tagid int, intvalue ${t.bigint}, floatvalue ${t.float}, stringvalue ${t.text}, datevalue ${t.date}, dataintegrity int, t_stamp ${t.bigint})`,
  `INSERT INTO sqlth_drv VALUES (1, 'site-gateway', 'default')`,
  `INSERT INTO sqlth_scinfo VALUES (1, 'default', 1)`,
  `INSERT INTO sqlth_te VALUES (10, 'Line2/Filler/Bowl_Level', 1, 1, 0, 0, ${ms("2026-09-20T00:00:00Z")})`,
  `INSERT INTO sqlth_te VALUES (11, 'line2/filler/bowl_level', 1, 1, 0, ${ms("2026-09-20T00:00:00Z")}, NULL)`,
  `INSERT INTO sqlth_te VALUES (12, 'line2/other', 1, 1, 0, 0, NULL)`,
  `INSERT INTO sqlth_partitions VALUES ('sqlt_data_1_2026_09', 1, ${ms("2026-09-01T00:00:00Z")}, ${ms("2026-10-01T00:00:00Z")}, 0, 0)`,
  `INSERT INTO sqlth_partitions VALUES ('sqlt_data_1_2026_10', 1, ${ms("2026-10-01T00:00:00Z")}, ${ms("2026-11-01T00:00:00Z")}, 0, 0)`,
  `INSERT INTO sqlt_data_1_2026_09 VALUES (10, NULL, 41.5, NULL, NULL, 192, ${ms("2026-09-10T08:00:00Z")})`,
  `INSERT INTO sqlt_data_1_2026_09 VALUES (11, NULL, 43.0, NULL, NULL, 192, ${ms("2026-09-25T08:00:00Z")})`,
  `INSERT INTO sqlt_data_1_2026_10 VALUES (11, 47, NULL, NULL, NULL, 100, ${ms("2026-10-05T08:00:00Z")})`,
  `INSERT INTO sqlt_data_1_2026_10 VALUES (12, NULL, 999, NULL, NULL, 192, ${ms("2026-10-05T09:00:00Z")})`,
];

async function check(h: IgnitionSqlHistorian) {
  const pts = await h.read([{ attributeId: "tag:bowl/value", address: "line2/filler/bowl_level" }], "2026-09-01T00:00:00Z", "2026-10-31T00:00:00Z", 1000);
  assert.deepEqual(pts.map((p) => [p.timestamp, p.value, p.quality]), [
    ["2026-09-10T08:00:00.000Z", 41.5, "Good"], ["2026-09-25T08:00:00.000Z", 43, "Good"], ["2026-10-05T08:00:00.000Z", 47, "Uncertain"],
  ]);
  const capped = await h.read([{ attributeId: "tag:bowl/value", address: "line2/filler/bowl_level" }], "2026-09-01T00:00:00Z", "2026-10-31T00:00:00Z", 2);
  assert.equal(capped.length, 2, "max points honoured");
}

test("MySQL-hosted Ignition historian", { skip: !MYSQL && "no NEXUS_TEST_MYSQL", timeout: 120_000 }, async () => {
  let c: mysql.Connection | null = null;
  for (let i = 0; i < 40 && !c; i++) { try { c = await mysql.createConnection(MYSQL!); } catch { await new Promise((r) => setTimeout(r, 2000)); } }
  assert.ok(c, "MySQL reachable");
  await c!.query("DROP DATABASE IF EXISTS hist"); await c!.query("CREATE DATABASE hist"); await c!.query("USE hist");
  for (const s of seed({ bigint: "bigint", float: "double", text: "varchar(255)", date: "datetime" })) await c!.query(s);
  await c!.end();
  const h = new IgnitionSqlHistorian("mysql-hist", MYSQL!, "default", "mysql");
  try { await check(h); } finally { await h.close(); }
});

test("SQL Server-hosted Ignition historian", { skip: !MSSQL && "no NEXUS_TEST_MSSQL", timeout: 180_000 }, async () => {
  const u = new URL(MSSQL!);
  const cfg = { server: u.hostname, port: Number(u.port), user: decodeURIComponent(u.username), password: decodeURIComponent(u.password), database: "master", options: { trustServerCertificate: true, encrypt: true } };
  let pool: mssql.ConnectionPool | null = null;
  for (let i = 0; i < 60 && !pool; i++) { try { pool = await new mssql.ConnectionPool(cfg).connect(); } catch { await new Promise((r) => setTimeout(r, 2000)); } }
  assert.ok(pool, "SQL Server reachable");
  await pool!.request().query("IF DB_ID('hist') IS NOT NULL BEGIN ALTER DATABASE hist SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE hist; END; CREATE DATABASE hist;");
  await pool!.close();
  const hp = await new mssql.ConnectionPool({ ...cfg, database: "hist" }).connect();
  for (const s of seed({ bigint: "bigint", float: "float", text: "nvarchar(255)", date: "datetime2" })) await hp.request().query(s);
  await hp.close();
  u.pathname = "/hist";
  const h = new IgnitionSqlHistorian("mssql-hist", u.toString(), "default", "mssql");
  try { await check(h); } finally { await h.close(); }
});

test("dialect SQL: list expansion and placeholders per database; SQL Server caps with TOP", async () => {
  const { expand, mssqlLimit } = await import("../src/history/ignition");
  const q = "SELECT a FROM t WHERE p = ? AND id IN (?) AND ts BETWEEN ? AND ? ORDER BY ts";
  assert.deepEqual(expand(q, ["x", [1, 2, 3], 10, 20], (n) => `$${n}`), { sql: "SELECT a FROM t WHERE p = $1 AND id IN ($2, $3, $4) AND ts BETWEEN $5 AND $6 ORDER BY ts", params: ["x", 1, 2, 3, 10, 20] });
  assert.equal(expand(q, ["x", [1, 2], 10, 20], () => "?").sql, "SELECT a FROM t WHERE p = ? AND id IN (?, ?) AND ts BETWEEN ? AND ? ORDER BY ts");
  const ms = expand(q, ["x", [7], 10, 20], (n) => `@p${n}`);
  assert.equal(ms.sql, "SELECT a FROM t WHERE p = @p1 AND id IN (@p2) AND ts BETWEEN @p3 AND @p4 ORDER BY ts");
  assert.equal(mssqlLimit(ms.sql, 500), "SELECT TOP (500) a FROM t WHERE p = @p1 AND id IN (@p2) AND ts BETWEEN @p3 AND @p4 ORDER BY ts");
  assert.equal(expand("WHERE id IN (?)", [[]], () => "?").sql, "WHERE id IN (NULL)", "an empty list matches nothing rather than breaking the SQL");
});
