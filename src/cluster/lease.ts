// The governor lease: which node may write (profile NX-51, NX-52).
//
// Reads and subscriptions are served by every node. Governed writes are serialised
// through one node, the lease holder. The lease is a row with an expiry and an
// epoch. Taking it over bumps the epoch, and every ledger write re-checks
// "holder = me AND epoch = mine" inside its own transaction. That check is the
// fencing token: a node that paused (GC, swap, a network stall) and wakes up
// believing it still leads cannot write, because the database no longer agrees.
//
// Locally, a node stops calling itself leader as soon as its last successful
// renewal is older than the lease TTL minus a safety margin. It does not wait to
// hear that it lost. A node that cannot reach the database is read-only for
// writes. That is the "fence itself on losing quorum" rule.

import type pg from "pg";
import { EventEmitter } from "node:events";

export interface LeaseOptions { ttlMs?: number; renewMs?: number; marginMs?: number }

export interface LeaseState { holder: string | null; holderUrl: string | null; epoch: number; expiresAt: number }

export class GovernorLease extends EventEmitter {
  private timer: NodeJS.Timeout | null = null;
  private heldEpoch: number | null = null;
  private lastRenew = 0;
  private ttl: number;
  private renewEvery: number;
  private margin: number;
  /** The last lease row seen, whoever holds it: used to forward writes. */
  state: LeaseState = { holder: null, holderUrl: null, epoch: 0, expiresAt: 0 };
  /** Test hook: pretend the database is unreachable from this node. */
  partitioned = false;

  constructor(private pool: pg.Pool, readonly nodeId: string, private url: () => string, o: LeaseOptions = {}) {
    super();
    this.ttl = o.ttlMs ?? 10_000;
    this.renewEvery = o.renewMs ?? 2_000;
    this.margin = o.marginMs ?? 2_000;
  }

  private stopped = false;

  start() {
    const loop = async () => {
      await this.tick().catch(() => {});
      if (!this.stopped) this.timer = setTimeout(loop, this.renewEvery);
    };
    void loop();
  }

  stop() { this.stopped = true; if (this.timer) clearTimeout(this.timer); this.timer = null; }

  /** True only while a renewal is recent enough that no other node can have taken over. */
  isLeader(now = Date.now()) {
    return this.heldEpoch !== null && now - this.lastRenew < this.ttl - this.margin;
  }

  epoch() { return this.heldEpoch; }

  /** Give the lease up at once (clean shutdown), so the standby does not wait out the TTL. */
  async release() {
    if (this.heldEpoch === null) return;
    const e = this.heldEpoch;
    this.heldEpoch = null;
    await this.pool.query(`UPDATE nexus_lease SET expires_at = 'epoch' WHERE name = 'governor' AND holder = $1 AND epoch = $2`, [this.nodeId, e]).catch(() => {});
  }

  /**
   * Re-read who holds the lease. Expiry is kept relative to the database's own
   * clock (remaining time, not its wall-clock timestamp), so clock skew between
   * this node and the database cannot make a live lease look expired.
   */
  async refresh(): Promise<LeaseState> {
    if (this.partitioned) throw new Error("partitioned");
    const s = await this.pool.query(`SELECT holder, holder_url, epoch, extract(epoch FROM (expires_at - now())) * 1000 AS remaining_ms FROM nexus_lease WHERE name = 'governor'`);
    const row = s.rows[0];
    this.state = { holder: row.holder, holderUrl: row.holder_url, epoch: Number(row.epoch), expiresAt: Date.now() + Number(row.remaining_ms) };
    return this.state;
  }

  async tick() {
    const wasLeader = this.isLeader();
    if (this.partitioned) throw new Error("partitioned");
    // Renew if ours, take over if expired, otherwise just read who holds it.
    const r = await this.pool.query(
      `UPDATE nexus_lease SET
         holder = $1, holder_url = $2, expires_at = now() + ($3 || ' milliseconds')::interval,
         epoch = CASE WHEN holder = $1 AND expires_at > now() THEN epoch ELSE epoch + 1 END
       WHERE name = 'governor' AND (holder = $1 OR expires_at <= now())
       RETURNING holder, holder_url, epoch, extract(epoch FROM (expires_at - now())) * 1000 AS remaining_ms`,
      [this.nodeId, this.url(), String(this.ttl)]);
    if (r.rowCount) {
      const row = r.rows[0];
      const epoch = Number(row.epoch);
      if (this.heldEpoch !== null && this.heldEpoch !== epoch) this.heldEpoch = null; // lost it in between
      this.heldEpoch = epoch;
      this.lastRenew = Date.now();
      this.state = { holder: row.holder, holderUrl: row.holder_url, epoch, expiresAt: Date.now() + Number(row.remaining_ms) };
    } else {
      this.heldEpoch = null;
      await this.refresh();
    }
    const nowLeader = this.isLeader();
    if (nowLeader !== wasLeader) this.emit(nowLeader ? "acquired" : "lost", this.state);
  }
}
