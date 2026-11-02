// History for series the nexus owns (spec §5, "Historical").
//
// Two stores behind one interface:
//
//   MemoryHistory  the short event-log window every node keeps (default 24 h,
//                  bounded per attribute). Enough to answer recent history with
//                  no database, and what phase 1 runs on by default.
//   PgHistory      PostgreSQL, a TimescaleDB hypertable when the extension is
//                  present. Long retention for owned series.
//
// A federated historian (the plant's own) is a third implementation of the same
// interface and is not built yet; until it is, a tag the nexus does not own has
// only the short window, and says so in /info rather than pretending otherwise.
//
// Reads are async because a real store is. The i3X projection is synchronous, so
// the transport prefetches the range a request names and hands the projection the
// result (see `server.ts`).

import type { VQT } from "../i3x/wire";
import type pg from "pg";

export interface HistoryStore {
  append(points: VQT[]): void;
  read(attributeIds: readonly string[], from: string, to: string, maxPoints?: number): Promise<VQT[]>;
  close?(): Promise<void>;
}

export interface MemoryHistoryOptions { windowMs?: number; maxPerAttribute?: number }

export class MemoryHistory implements HistoryStore {
  private series = new Map<string, VQT[]>();
  private windowMs: number;
  private maxPer: number;

  constructor(o: MemoryHistoryOptions = {}) {
    this.windowMs = o.windowMs ?? 24 * 3600_000;
    this.maxPer = o.maxPerAttribute ?? 10_000;
  }

  append(points: VQT[]) {
    for (const p of points) {
      let s = this.series.get(p.attributeId);
      if (!s) { s = []; this.series.set(p.attributeId, s); }
      // Out-of-order points (late arrivals, backfill) are inserted in place, so reads stay ordered.
      if (s.length && s[s.length - 1].timestamp > p.timestamp) {
        const i = s.findIndex((x) => x.timestamp > p.timestamp);
        s.splice(i, 0, p);
      } else s.push(p);
      const cutoff = new Date(Date.now() - this.windowMs).toISOString();
      let drop = 0;
      while (drop < s.length && s[drop].timestamp < cutoff) drop++;
      if (s.length - drop > this.maxPer) drop = s.length - this.maxPer;
      if (drop) s.splice(0, drop);
    }
  }

  readSync(attributeIds: readonly string[], from: string, to: string, maxPoints = 100_000): VQT[] {
    const lo = Date.parse(from), hi = Date.parse(to);
    const out: VQT[] = [];
    for (const id of attributeIds) {
      for (const p of this.series.get(id) ?? []) {
        const t = Date.parse(p.timestamp);
        if (t >= lo && t <= hi) out.push(p);
        if (out.length >= maxPoints) return out;
      }
    }
    return out;
  }

  async read(attributeIds: readonly string[], from: string, to: string, maxPoints?: number) {
    return this.readSync(attributeIds, from, to, maxPoints);
  }
}

/**
 * PostgreSQL history. Appends are buffered and flushed every `flushMs`, so a
 * burst of changes is one INSERT, not thousands. A flush that fails keeps its
 * rows and retries, bounded by `maxBuffered`; past that the oldest are dropped
 * and counted, never silently.
 */
export class PgHistory implements HistoryStore {
  private buf: VQT[] = [];
  private timer: NodeJS.Timeout;
  dropped = 0;

  private constructor(private pool: pg.Pool, flushMs: number, private maxBuffered: number) {
    this.timer = setInterval(() => { void this.flush(); }, flushMs);
  }

  static async open(pool: pg.Pool, o: { flushMs?: number; maxBuffered?: number } = {}) {
    await pool.query(`CREATE TABLE IF NOT EXISTS nexus_history (
      attribute_id text NOT NULL, ts timestamptz NOT NULL, value jsonb, quality text NOT NULL)`);
    // Unique on (attribute, source time): both nodes of a pair record the same source
    // samples, and the second copy of a sample is dropped, not stored twice.
    await pool.query(`CREATE UNIQUE INDEX IF NOT EXISTS nexus_history_attr_ts_u ON nexus_history (attribute_id, ts)`);
    const ext = await pool.query(`SELECT 1 FROM pg_extension WHERE extname = 'timescaledb'`);
    if (ext.rowCount) await pool.query(`SELECT create_hypertable('nexus_history', 'ts', if_not_exists => true, migrate_data => true)`);
    return new PgHistory(pool, o.flushMs ?? 500, o.maxBuffered ?? 200_000);
  }

  append(points: VQT[]) {
    this.buf.push(...points);
    if (this.buf.length > this.maxBuffered) {
      const over = this.buf.length - this.maxBuffered;
      this.buf.splice(0, over);
      this.dropped += over;
    }
  }

  async flush() {
    if (!this.buf.length) return;
    const rows = this.buf.splice(0, 5_000);
    const vals: unknown[] = [];
    const tuples = rows.map((r, i) => {
      vals.push(r.attributeId, r.timestamp, JSON.stringify(r.value), r.quality);
      return `($${i * 4 + 1}, $${i * 4 + 2}, $${i * 4 + 3}::jsonb, $${i * 4 + 4})`;
    });
    try {
      await this.pool.query(`INSERT INTO nexus_history (attribute_id, ts, value, quality) VALUES ${tuples.join(",")} ON CONFLICT DO NOTHING`, vals);
    } catch {
      this.buf.unshift(...rows);
    }
  }

  async read(attributeIds: readonly string[], from: string, to: string, maxPoints = 100_000) {
    const r = await this.pool.query(
      `SELECT attribute_id, ts, value, quality FROM nexus_history
        WHERE attribute_id = ANY($1) AND ts BETWEEN $2 AND $3 ORDER BY attribute_id, ts LIMIT $4`,
      [attributeIds, from, to, maxPoints]);
    return r.rows.map((x) => ({
      attributeId: x.attribute_id as string, timestamp: (x.ts as Date).toISOString(),
      value: x.value as VQT["value"], quality: x.quality as VQT["quality"],
    }));
  }

  async close() { clearInterval(this.timer); await this.flush(); }
}
