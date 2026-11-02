import { existsSync, mkdtempSync, readFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { randomBytes } from "node:crypto";
import pg from "pg";
import { Nexus } from "../src/nexus";
import { exampleConfig, principals, TOKENS } from "./helpers";
import type { SiteConfig } from "../src/model/config";

/** Cluster tests need a real Postgres: NEXUS_TEST_PG, or .local/pg-url. Skipped without one. */
export const PG_URL = process.env.NEXUS_TEST_PG ?? (existsSync(".local/pg-url") ? readFileSync(".local/pg-url", "utf8").trim() : null);

/** A fresh schema per test, so runs never see each other's ledger or lease. */
export async function freshSchema(): Promise<{ url: string; drop: () => Promise<void> }> {
  const schema = `t_${randomBytes(5).toString("hex")}`;
  const admin = new pg.Client(PG_URL!);
  await admin.connect();
  await admin.query(`CREATE SCHEMA ${schema}`);
  await admin.end();
  const u = new URL(PG_URL!);
  u.searchParams.set("options", `-c search_path=${schema}`);
  return {
    url: u.toString(),
    drop: async () => {
      const c = new pg.Client(PG_URL!);
      await c.connect();
      try { await c.query(`DROP SCHEMA IF EXISTS ${schema} CASCADE`); } finally { await c.end(); }
    },
  };
}

export const FAST_LEASE = { ttlMs: 3_000, renewMs: 400, marginMs: 600 };

export async function startNode(nodeId: string, pgUrl: string, cfg: SiteConfig = exampleConfig(), dataDir?: string, people?: import("../src/core/auth").PrincipalDef[]) {
  const dir = dataDir ?? mkdtempSync(join(tmpdir(), `nexus-${nodeId}-`));
  const nexus = new Nexus({
    config: cfg, principals: people ?? principals(cfg), dataDir: dir, port: 0, pgUrl,
    rate: { perSecond: 5000, burst: 5000 }, mediaSigningKey: "test-media-key",
    cluster: { nodeId, secret: "test-cluster-secret", lease: FAST_LEASE, heartbeatMs: 500 },
  });
  const port = await nexus.start();
  const base = `http://127.0.0.1:${port}/v1`;
  const call = async (method: string, path: string, body?: unknown, token = TOKENS.reader, headers: Record<string, string> = {}) => {
    const r = await fetch(base + path, {
      method, headers: { authorization: `Bearer ${token}`, "content-type": "application/json", ...headers },
      body: body === undefined ? undefined : JSON.stringify(body), signal: AbortSignal.timeout(15_000),
    });
    return { status: r.status, headers: r.headers, body: (await r.json().catch(() => null)) as any };
  };
  const value = async (id: string) => (await call("POST", "/objects/value", { elementIds: [id] })).body?.results?.[0];
  return { nexus, base, call, value, dir, nodeId };
}

export type Node = Awaited<ReturnType<typeof startNode>>;

export const leaderOf = (nodes: Node[]) => nodes.filter((n) => n.nexus.lease?.isLeader());
