// History federation: the plant's own historian answers /objects/history for the
// tags it holds (profile §5, "Historical"; NX-4). The nexus keeps no copy.
//
// This adapter reads Ignition's SQL historian layout, the most common one on the
// plant floor, on whichever database the gateway writes to (PostgreSQL, MySQL /
// MariaDB, or Microsoft SQL Server):
//
//   sqlth_drv         (id, name, provider)            one row per gateway/provider
//   sqlth_scinfo      (id, scname, drvid)             scan classes
//   sqlth_te          (id, tagpath, scid, datatype, created, retired)
//                     a tag can have several rows over its life (retired ids keep their history)
//   sqlth_partitions  (pname, drvid, start_time, end_time)  one data table per period
//   sqlt_data_*       (tagid, intvalue, floatvalue, stringvalue, datevalue, dataintegrity, t_stamp)
//                     t_stamp and partition bounds are epoch milliseconds
//
// Quality is the classic OPC code in `dataintegrity`: 192 and up Good, 64–191
// Uncertain, below 64 Bad.
//
// A historian that cannot answer makes the request fail. It never makes the series
// empty, which a client would read as "nothing happened" (NX-4).

import pg from "pg";
import mysql from "mysql2/promise";
import mssql from "mssql";
import type { VQT } from "../i3x/wire";

export type HistorianDialect = "postgres" | "mysql" | "mssql";

export interface HistorianAdapter {
  readonly id: string;
  /** addresses: historian tag paths. Returns points keyed back to the given attribute ids. */
  read(map: Array<{ attributeId: string; address: string }>, from: string, to: string, maxPoints: number): Promise<VQT[]>;
  close(): Promise<void>;
}

const IDENT = /^[a-z_][a-z0-9_]*$/i;

export function qualityOfIntegrity(code: number | null): VQT["quality"] {
  if (code === null || code === undefined) return "Uncertain";
  if (code >= 192) return "Good";
  if (code >= 64) return "Uncertain";
  return "Bad";
}

/** One query shape for three databases: `?` placeholders, list parameters expanded. */
interface Db {
  query(sql: string, params: unknown[]): Promise<Array<Record<string, any>>>;
  limit(sql: string, n: number): string;
  close(): Promise<void>;
}

export function expand(sql: string, params: unknown[], ph: (i: number) => string): { sql: string; params: unknown[] } {
  const out: unknown[] = [];
  let i = 0;
  const text = sql.replace(/\?/g, () => {
    const p = params[i++];
    if (Array.isArray(p)) { if (!p.length) return "NULL"; return p.map((v) => { out.push(v); return ph(out.length); }).join(", "); }
    out.push(p);
    return ph(out.length);
  });
  return { sql: text, params: out };
}

function postgresDb(url: string): Db {
  const pool = new pg.Pool({ connectionString: url, max: 4, statement_timeout: 15_000 } as pg.PoolConfig);
  return {
    async query(sql, params) { const e = expand(sql, params, (n) => `$${n}`); return (await pool.query(e.sql, e.params)).rows; },
    limit: (sql, n) => `${sql} LIMIT ${n}`,
    close: () => pool.end(),
  };
}

function mysqlDb(url: string): Db {
  const pool = mysql.createPool({ uri: url, connectionLimit: 4, supportBigNumbers: true, bigNumberStrings: false });
  return {
    async query(sql, params) { const e = expand(sql, params, () => "?"); const [rows] = await pool.query(e.sql, e.params); return rows as Array<Record<string, any>>; },
    limit: (sql, n) => `${sql} LIMIT ${n}`,
    close: () => pool.end(),
  };
}

/** SQL Server has no LIMIT: the cap goes in as TOP right after SELECT. */
export const mssqlLimit = (sql: string, n: number) => sql.replace(/^\s*SELECT /i, `SELECT TOP (${n}) `);

function mssqlDb(url: string): Db {
  // mssql://user:pass@host:port/database
  const u = new URL(url);
  const pool = new mssql.ConnectionPool({
    server: u.hostname, port: Number(u.port || 1433), user: decodeURIComponent(u.username), password: decodeURIComponent(u.password),
    database: u.pathname.slice(1), requestTimeout: 15_000, pool: { max: 4 },
    options: { encrypt: u.searchParams.get("encrypt") !== "false", trustServerCertificate: u.searchParams.get("trustServerCertificate") === "true" },
  });
  const ready = pool.connect();
  return {
    async query(sql, params) {
      await ready;
      const e = expand(sql, params, (n) => `@p${n}`);
      const req = pool.request();
      e.params.forEach((v, i) => req.input(`p${i + 1}`, v as never));
      return (await req.query(e.sql)).recordset as Array<Record<string, any>>;
    },
    limit: mssqlLimit,
    close: async () => { await ready.catch(() => {}); await pool.close(); },
  };
}

export class IgnitionSqlHistorian implements HistorianAdapter {
  private db: Db;
  constructor(readonly id: string, url: string, private provider: string, dialect: HistorianDialect = "postgres") {
    this.db = dialect === "mysql" ? mysqlDb(url) : dialect === "mssql" ? mssqlDb(url) : postgresDb(url);
  }

  async read(map: Array<{ attributeId: string; address: string }>, from: string, to: string, maxPoints: number): Promise<VQT[]> {
    if (!map.length) return [];
    const lo = Date.parse(from), hi = Date.parse(to);
    const paths = [...new Set(map.map((m) => m.address.toLowerCase()))];
    // Every tag id that ever carried these paths on this provider, retired ones included.
    const te = await this.db.query(
      `SELECT te.id AS id, lower(te.tagpath) AS tagpath FROM sqlth_te te
         JOIN sqlth_scinfo sc ON sc.id = te.scid JOIN sqlth_drv d ON d.id = sc.drvid
        WHERE d.provider = ? AND lower(te.tagpath) IN (?)`, [this.provider, paths]);
    if (!te.length) return [];
    const pathOf = new Map(te.map((r) => [Number(r.id), String(r.tagpath)]));
    const parts = await this.db.query(
      `SELECT p.pname AS pname FROM sqlth_partitions p JOIN sqlth_drv d ON d.id = p.drvid
        WHERE d.provider = ? AND p.end_time >= ? AND p.start_time <= ? ORDER BY p.start_time`, [this.provider, lo, hi]);
    const out: VQT[] = [];
    for (const { pname } of parts) {
      if (!IDENT.test(pname)) throw new Error(`historian ${this.id}: refusing partition name ${pname}`);
      const rows = await this.db.query(this.db.limit(
        `SELECT tagid, intvalue, floatvalue, stringvalue, datevalue, dataintegrity, t_stamp FROM ${pname}
          WHERE tagid IN (?) AND t_stamp BETWEEN ? AND ? ORDER BY t_stamp`, maxPoints - out.length),
        [[...pathOf.keys()], lo, hi]);
      for (const row of rows) {
        const path = pathOf.get(Number(row.tagid))!;
        const value = row.floatvalue ?? (row.intvalue !== null && row.intvalue !== undefined ? Number(row.intvalue) : null) ?? row.stringvalue ?? (row.datevalue ? new Date(row.datevalue).toISOString() : null);
        for (const m of map) if (m.address.toLowerCase() === path) {
          out.push({ attributeId: m.attributeId, value, quality: qualityOfIntegrity(row.dataintegrity === null ? null : Number(row.dataintegrity)), timestamp: new Date(Number(row.t_stamp)).toISOString() });
        }
      }
      if (out.length >= maxPoints) break;
    }
    return out.sort((a, b) => a.timestamp.localeCompare(b.timestamp));
  }

  close() { return this.db.close(); }
}
