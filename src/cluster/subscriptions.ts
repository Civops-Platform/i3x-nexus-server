// Subscriptions shared by a redundant pair (profile NX-54).
//
// The wire layer works on an in-memory SubscriptionStore. In a cluster each
// subscription request loads only the subscriptions it names, under row locks,
// runs the wire layer on that small store, and writes back what changed, all in
// one transaction. Either node can then answer any subscription call, and a client
// that fails over finds its subscription, its monitored set and its queued batches
// where it left them.

import type pg from "pg";
import {
  createSubscriptionStore, deserializeSubscriptionStore, serializeSubscriptionStore, SUBSCRIPTION_LIMITS,
  type Subscription, type SubscriptionStore,
} from "../i3x/wire";

export async function withSubscriptions<T>(
  pool: pg.Pool, ids: string[], fn: (store: SubscriptionStore) => T, opts: { creating?: boolean } = {},
): Promise<{ result: T; tooMany: boolean }> {
  const c = await pool.connect();
  try {
    await c.query("BEGIN");
    if (opts.creating) {
      const n = await c.query(`SELECT count(*)::int AS n FROM nexus_subscriptions`);
      if (n.rows[0].n >= SUBSCRIPTION_LIMITS.maxSubscriptions) { await c.query("ROLLBACK"); return { result: undefined as T, tooMany: true }; }
    }
    const r = ids.length
      ? await c.query(`SELECT data FROM nexus_subscriptions WHERE id = ANY($1) FOR UPDATE`, [ids])
      : { rows: [] as Array<{ data: unknown }> };
    const store = deserializeSubscriptionStore(JSON.stringify(r.rows.map((x) => x.data)));
    const before = new Set(store.subs.keys());
    const result = fn(store);
    const after = JSON.parse(serializeSubscriptionStore(store)) as Array<{ subscriptionId: string; clientId: string }>;
    for (const s of after) {
      await c.query(
        `INSERT INTO nexus_subscriptions (id, client_id, data, updated_at) VALUES ($1, $2, $3, now())
         ON CONFLICT (id) DO UPDATE SET data = EXCLUDED.data, updated_at = now()`,
        [s.subscriptionId, s.clientId, JSON.stringify(s)]);
    }
    const gone = [...before].filter((id) => !store.subs.has(id));
    if (gone.length) await c.query(`DELETE FROM nexus_subscriptions WHERE id = ANY($1)`, [gone]);
    await c.query("COMMIT");
    return { result, tooMany: false };
  } catch (e) {
    await c.query("ROLLBACK").catch(() => {});
    throw e;
  } finally {
    c.release();
  }
}

export async function readSubscription(pool: pg.Pool, id: string): Promise<Subscription | null> {
  const r = await pool.query(`SELECT data FROM nexus_subscriptions WHERE id = $1`, [id]);
  if (!r.rowCount) return null;
  return deserializeSubscriptionStore(JSON.stringify([r.rows[0].data])).subs.get(id) ?? null;
}

export async function expireSubscriptions(pool: pg.Pool) {
  await pool.query(`DELETE FROM nexus_subscriptions WHERE updated_at < now() - ($1 || ' milliseconds')::interval`, [String(SUBSCRIPTION_LIMITS.idleExpiryMs)]);
}

export const emptyStore = createSubscriptionStore;
