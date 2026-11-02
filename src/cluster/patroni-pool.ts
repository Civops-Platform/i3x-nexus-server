// A Postgres pool that follows the Patroni leader (profile §9: the shared database
// is itself redundant).
//
// node-postgres connects to one host. Under Patroni the writable primary moves on
// failover, so this pool asks each Patroni member's REST API which one is primary
// (`GET /primary` answers 200 only on the leader), keeps a pool to it, and on a
// connection error or a "read-only" refusal looks again and retries the call once.
// It also re-checks every few seconds, so a planned switchover is followed without
// waiting for an error. While no member is primary (mid-failover) calls fail fast,
// which the nexus already treats as "no write leader" (503 + Retry-After).

import pg from "pg";
import { EventEmitter } from "node:events";

export interface PatroniMember { pg: string; api: string }

const RETRYABLE = new Set(["ECONNREFUSED", "ECONNRESET", "ETIMEDOUT", "EPIPE", "57P01", "57P02", "57P03", "25006", "08006", "08001", "08003"]);
const retryable = (e: unknown) => {
  const x = e as { code?: string; message?: string };
  return RETRYABLE.has(String(x?.code)) || /terminat|read-only|Connection terminated|not accept|recovery/i.test(String(x?.message));
};

export class PatroniPool extends EventEmitter {
  private pool: pg.Pool | null = null;
  private leader: string | null = null;
  private timer: NodeJS.Timeout;
  private finding: Promise<pg.Pool> | null = null;

  /** `url` carries credentials and database; its host is replaced by the leader's. */
  constructor(private url: string, private members: PatroniMember[], private opts: pg.PoolConfig = {}, everyMs = 3_000) {
    super();
    this.timer = setInterval(() => void this.find().catch(() => {}), everyMs);
    this.timer.unref();
  }

  currentLeader() { return this.leader; }

  private async probe(m: PatroniMember) {
    try { const r = await fetch(`${m.api}/primary`, { signal: AbortSignal.timeout(2_000) }); return r.status === 200; } catch { return false; }
  }

  /** Find the primary; replace the pool if it moved. */
  find(): Promise<pg.Pool> {
    this.finding ??= (async () => {
      try {
        const hits = await Promise.all(this.members.map(async (m) => ((await this.probe(m)) ? m : null)));
        const m = hits.find(Boolean);
        if (!m) throw Object.assign(new Error("no Patroni member is primary right now"), { code: "57P03" });
        if (m.pg !== this.leader || !this.pool) {
          const old = this.pool;
          const u = new URL(this.url);
          const [host, port] = m.pg.split(":");
          u.hostname = host; u.port = port;
          this.pool = new pg.Pool({ ...this.opts, connectionString: u.toString() });
          this.pool.on("error", (e) => this.emit("error", e));
          const prev = this.leader;
          this.leader = m.pg;
          if (prev) this.emit("switched", { from: prev, to: m.pg });
          void old?.end().catch(() => {});
        }
        return this.pool!;
      } finally { this.finding = null; }
    })();
    return this.finding;
  }

  private async withRetry<T>(fn: (p: pg.Pool) => Promise<T>): Promise<T> {
    const p = this.pool ?? (await this.find());
    try { return await fn(p); } catch (e) {
      if (!retryable(e)) throw e;
      return fn(await this.find());
    }
  }

  query(text: string, values?: unknown[]): Promise<pg.QueryResult> {
    return this.withRetry((p) => p.query(text, values as never[]));
  }

  connect(): Promise<pg.PoolClient> {
    return this.withRetry((p) => p.connect());
  }

  async end() {
    clearInterval(this.timer);
    await this.pool?.end().catch(() => {});
  }
}

/** `postgres://u:p@h1:5432,h2:5432/db` + Patroni API URLs → members, in order. */
export function parseMembers(pgUrl: string, apis: string[]): { url: string; members: PatroniMember[] } {
  const m = /^(postgres(?:ql)?:\/\/[^@]+@)([^/]+)(\/.*)$/.exec(pgUrl);
  if (!m) throw new Error("NEXUS_PG_URL is not a postgres:// URL");
  const hosts = m[2].split(",");
  if (hosts.length !== apis.length) throw new Error(`NEXUS_PG_URL names ${hosts.length} hosts but NEXUS_PG_PATRONI names ${apis.length} APIs`);
  return { url: `${m[1]}${hosts[0]}${m[3]}`, members: hosts.map((h, i) => ({ pg: h, api: apis[i].replace(/\/+$/, "") })) };
}
