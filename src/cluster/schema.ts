// Shared state for a redundant site nexus (profile §9, Level 2).
//
// Both nodes hold their own source sessions and their own last-value cache, so
// realtime values need no replication: each node reads the devices itself. What
// must be shared is everything a client or an auditor expects to survive a node:
// the record ledger, subscriptions, the media index, and who holds the governor.
//
// PostgreSQL holds it. In production that Postgres is itself redundant (Patroni
// with a three-member consensus store, the third member being the witness); this
// code treats it as one logical database and does not care which replica leads.

import type pg from "pg";

export const SCHEMA = `
CREATE TABLE IF NOT EXISTS nexus_records (
  seq          bigserial PRIMARY KEY,
  element_id   text        NOT NULL,
  version      integer     NOT NULL,
  value        jsonb,
  ts           timestamptz NOT NULL,
  written_at   timestamptz NOT NULL,
  principal    text        NOT NULL,
  prev_hash    text        NOT NULL,
  hash         text        NOT NULL,
  idempotency_key text     UNIQUE,
  lease_epoch  bigint      NOT NULL,
  UNIQUE (element_id, version)
);
CREATE TABLE IF NOT EXISTS nexus_chain (id int PRIMARY KEY CHECK (id = 1), head text NOT NULL);
INSERT INTO nexus_chain (id, head) VALUES (1, repeat('0', 64)) ON CONFLICT DO NOTHING;

CREATE TABLE IF NOT EXISTS nexus_lease (
  name        text PRIMARY KEY,
  holder      text,
  holder_url  text,
  epoch       bigint      NOT NULL DEFAULT 0,
  expires_at  timestamptz NOT NULL DEFAULT 'epoch'
);
INSERT INTO nexus_lease (name) VALUES ('governor') ON CONFLICT DO NOTHING;

CREATE TABLE IF NOT EXISTS nexus_nodes (
  node_id    text PRIMARY KEY,
  url        text        NOT NULL,
  heartbeat  timestamptz NOT NULL,
  started_at timestamptz NOT NULL
);

CREATE TABLE IF NOT EXISTS nexus_subscriptions (
  id          text PRIMARY KEY,
  client_id   text        NOT NULL,
  data        jsonb       NOT NULL,
  updated_at  timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS nexus_media (
  sha        text PRIMARY KEY,
  item       jsonb  NOT NULL,
  holders    text[] NOT NULL DEFAULT '{}'
);
`;

export async function migrate(pool: pg.Pool) {
  // Serialise concurrent first starts of two nodes.
  const c = await pool.connect();
  try {
    await c.query("SELECT pg_advisory_lock(727274)");
    await c.query(SCHEMA);
  } finally {
    await c.query("SELECT pg_advisory_unlock(727274)").catch(() => {});
    c.release();
  }
}
