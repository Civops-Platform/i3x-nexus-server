// Records the nexus owns: decision records, outcome observations, outbound
// signals, anything a client publishes into a namespace it owns (tier W0).
//
// Every write is a new version; nothing is updated in place. A record's i3X
// history is its version list. Every version carries the hash of the version
// written before it, anywhere in the store, so an edited, removed or reordered
// version breaks the chain and `verify()` says where.
//
// Two backends behind one store:
//
//   file      a JSONL ledger, replayed on start. One node (profile Level 1).
//   postgres  a shared ledger for a redundant pair (Level 2). Only the governor
//             lease holder appends, and the append re-checks the lease epoch in
//             the same transaction. That is the fence: a node that lost the lease
//             cannot add a version, even if it has not noticed yet. Every node
//             keeps an in-memory mirror, filled from the table at start and kept
//             current by LISTEN/NOTIFY.
//
// Reads (`current`, `history`) are always served from the mirror, so they stay
// synchronous and cost nothing on the request path.

import { appendFileSync, existsSync, mkdirSync, readFileSync } from "node:fs";
import { createHash } from "node:crypto";
import { dirname } from "node:path";
import type pg from "pg";

export interface RecordVersion {
  elementId: string;
  version: number;
  value: unknown;
  /** When the record says it was true (the writer's timestamp, or receipt time). */
  timestamp: string;
  writtenAt: string;
  principal: string;
  prevHash: string;
  hash: string;
}

export const GENESIS = "0".repeat(64);

/** JSON with object keys sorted, so a value hashes the same after a jsonb round trip (jsonb reorders keys). */
export function canonical(v: unknown): string {
  if (v === null || typeof v !== "object") return JSON.stringify(v) ?? "null";
  if (Array.isArray(v)) return `[${v.map(canonical).join(",")}]`;
  const o = v as Record<string, unknown>;
  return `{${Object.keys(o).filter((k) => o[k] !== undefined).sort().map((k) => `${JSON.stringify(k)}:${canonical(o[k])}`).join(",")}}`;
}

export const hashOf = (v: Omit<RecordVersion, "hash">) =>
  createHash("sha256").update(canonical([v.elementId, v.version, v.value, v.timestamp, v.writtenAt, v.principal, v.prevHash])).digest("hex");

/** Thrown when this node may not write: it does not hold the governor lease. */
export class NotLeaderError extends Error {}

/** Walk a chain in write order. Returns the first broken position (1-based), or null. */
export function verifyChain(rows: RecordVersion[]): { line: number; reason: string } | null {
  let prev = GENESIS;
  for (const [i, v] of rows.entries()) {
    if (v.prevHash !== prev) return { line: i + 1, reason: "previous-hash mismatch (a version was removed or reordered)" };
    const { hash, ...rest } = v;
    if (hashOf(rest) !== hash) return { line: i + 1, reason: "hash mismatch (a version was edited)" };
    prev = hash;
  }
  return null;
}

interface Backend {
  append(draft: (prevHash: string) => RecordVersion, idempotencyKey?: string): Promise<RecordVersion>;
  verify(): Promise<{ line: number; reason: string } | null>;
  /** Versions in the ledger and the hash of the last one (what an anchor signs). */
  head(): Promise<{ count: number; head: string }>;
}

export class RecordStore {
  private versions = new Map<string, RecordVersion[]>();
  private backend: Backend;
  private lastHash = GENESIS;

  /** Single-node file ledger. `null` keeps it in memory only. */
  constructor(private file: string | null) {
    if (file && existsSync(file)) {
      for (const line of readFileSync(file, "utf8").split("\n")) {
        if (!line.trim()) continue;
        const v = JSON.parse(line) as RecordVersion;
        this.index(v);
        this.lastHash = v.hash;
      }
    }
    if (file) mkdirSync(dirname(file), { recursive: true });
    this.backend = {
      append: async (draft) => {
        const v = draft(this.lastHash);
        if (this.file) appendFileSync(this.file, JSON.stringify(v) + "\n");
        this.lastHash = v.hash;
        return v;
      },
      verify: async () => (this.file && existsSync(this.file)
        ? verifyChain(readFileSync(this.file, "utf8").split("\n").filter((l) => l.trim()).map((l) => JSON.parse(l)))
        : null),
      head: async () => ({ count: this.count(), head: this.lastHash }),
    };
  }

  /** Shared ledger. `fence()` returns this node's lease epoch, or null when it is not the leader. */
  static async postgres(pool: pg.Pool, fence: () => number | null, nodeId: string): Promise<RecordStore> {
    const s = new RecordStore(null);
    let lastSeq = 0;
    const catchUp = async () => {
      const r = await pool.query(`SELECT * FROM nexus_records WHERE seq > $1 ORDER BY seq`, [lastSeq]);
      for (const row of r.rows) {
        s.index(rowToVersion(row));
        lastSeq = Number(row.seq);
      }
    };
    await catchUp();
    // LISTEN on a dedicated connection. A dropped one (the primary failed over) is
    // re-opened on the new primary; the poll below covers the gap.
    let pending: Promise<void> | null = null;
    const pull = () => { pending ??= catchUp().catch(() => {}).finally(() => { pending = null; }); return pending; };
    let listener: pg.PoolClient | null = null;
    let closed = false;
    const listen = async () => {
      try {
        const c = await pool.connect();
        c.on("error", () => { try { c.release(true); } catch { /* gone */ } if (listener === c) listener = null; if (!closed) setTimeout(() => void listen(), 1_000); });
        c.on("notification", () => void pull());
        await c.query("LISTEN nexus_records");
        listener = c;
      } catch { if (!closed) setTimeout(() => void listen(), 1_000); }
    };
    await listen();
    const poll = setInterval(() => void pull(), 2_000);
    poll.unref();
    s.closeHook = async () => { closed = true; clearInterval(poll); try { listener?.release(); } catch { /* gone */ } };

    s.backend = {
      append: async (draft, idempotencyKey) => {
        const epoch = fence();
        if (epoch === null) throw new NotLeaderError(`${nodeId} does not hold the governor lease`);
        const c = await pool.connect();
        try {
          await c.query("BEGIN");
          // The fence: the lease must still be ours, at our epoch, inside this transaction.
          const lease = await c.query(`SELECT 1 FROM nexus_lease WHERE name = 'governor' AND holder = $1 AND epoch = $2 AND expires_at > now() FOR SHARE`, [nodeId, epoch]);
          if (!lease.rowCount) throw new NotLeaderError(`${nodeId} lost the governor lease (epoch ${epoch})`);
          if (idempotencyKey) {
            const dup = await c.query(`SELECT * FROM nexus_records WHERE idempotency_key = $1`, [idempotencyKey]);
            if (dup.rowCount) { await c.query("ROLLBACK"); return rowToVersion(dup.rows[0]); }
          }
          const head = await c.query(`SELECT head FROM nexus_chain WHERE id = 1 FOR UPDATE`);
          const v = draft(head.rows[0].head as string);
          // The version number comes from the table, not the mirror, which may lag by a notify.
          const cur = await c.query(`SELECT coalesce(max(version), 0) AS v FROM nexus_records WHERE element_id = $1`, [v.elementId]);
          const version = Number(cur.rows[0].v) + 1;
          const fixed = version === v.version ? v : (() => { const { hash: _h, ...rest } = v; const b = { ...rest, version }; return { ...b, hash: hashOf(b) }; })();
          const ins = await c.query(
            `INSERT INTO nexus_records (element_id, version, value, ts, written_at, principal, prev_hash, hash, idempotency_key, lease_epoch)
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) RETURNING seq`,
            [fixed.elementId, fixed.version, JSON.stringify(fixed.value), fixed.timestamp, fixed.writtenAt, fixed.principal, fixed.prevHash, fixed.hash, idempotencyKey ?? null, epoch]);
          await c.query(`UPDATE nexus_chain SET head = $1 WHERE id = 1`, [fixed.hash]);
          await c.query(`SELECT pg_notify('nexus_records', $1)`, [String(ins.rows[0].seq)]);
          await c.query("COMMIT");
          return fixed;
        } catch (e) {
          await c.query("ROLLBACK").catch(() => {});
          throw e;
        } finally {
          c.release();
        }
      },
      verify: async () => {
        const r = await pool.query(`SELECT * FROM nexus_records ORDER BY seq`);
        return verifyChain(r.rows.map(rowToVersion));
      },
      head: async () => {
        const r = await pool.query(`SELECT count(*) AS n, coalesce((array_agg(hash ORDER BY seq DESC))[1], '${GENESIS}') AS head FROM nexus_records`);
        return { count: Number(r.rows[0].n), head: r.rows[0].head as string };
      },
    };
    return s;
  }

  private closeHook: (() => Promise<void>) | null = null;
  async close() { await this.closeHook?.(); }

  /** Called when an element id is seen for the first time (the address space grows). */
  onNewElement: (elementId: string) => void = () => {};

  private index(v: RecordVersion) {
    const isNew = !this.versions.has(v.elementId);
    const list = this.versions.get(v.elementId) ?? [];
    if (list.some((x) => x.version === v.version)) return;
    list.push(v);
    list.sort((a, b) => a.version - b.version);
    this.versions.set(v.elementId, list);
    if (isNew) this.onNewElement(v.elementId);
  }

  current(elementId: string): RecordVersion | undefined { return this.versions.get(elementId)?.at(-1); }
  history(elementId: string): RecordVersion[] { return this.versions.get(elementId) ?? []; }
  ids(): string[] { return [...this.versions.keys()]; }
  count(): number { let n = 0; for (const l of this.versions.values()) n += l.length; return n; }

  async write(elementId: string, value: unknown, principal: string, timestamp?: string, idempotencyKey?: string): Promise<RecordVersion> {
    const writtenAt = new Date().toISOString();
    const v = await this.backend.append((prevHash) => {
      const base = {
        elementId, value, principal, writtenAt, prevHash,
        version: (this.current(elementId)?.version ?? 0) + 1,
        timestamp: timestamp ?? writtenAt,
      };
      return { ...base, hash: hashOf(base) };
    }, idempotencyKey);
    this.index(v);
    return v;
  }

  verify() { return this.backend.verify(); }
  head() { return this.backend.head(); }
}

function rowToVersion(r: Record<string, unknown>): RecordVersion {
  return {
    elementId: r.element_id as string,
    version: Number(r.version),
    value: r.value,
    timestamp: (r.ts as Date).toISOString(),
    writtenAt: (r.written_at as Date).toISOString(),
    principal: r.principal as string,
    prevHash: r.prev_hash as string,
    hash: r.hash as string,
  };
}
