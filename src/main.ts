// Run one site nexus from the environment.
//
//   NEXUS_CONFIG        site model JSON (default config/example-site.json)
//   NEXUS_PRINCIPALS    principals JSON file; tokens stored as SHA-256 (see scripts/principal.ts)
//   NEXUS_DATA          data directory: records ledger, media (default ./data)
//   NEXUS_PORT/HOST     listen address (default 127.0.0.1:8400)
//   NEXUS_PUBLIC_BASE   public URL incl. /v1, for signed media links
//   NEXUS_PG_URL        optional PostgreSQL/TimescaleDB for owned history (several hosts: h1:p,h2:p)
//   NEXUS_PG_PATRONI    Patroni REST URLs, one per host above: follow the elected primary
//   NEXUS_MEDIA_KEY     HMAC key for media URLs (random per start if unset: links die on restart)
//   NEXUS_NODE_ID       Level 2: this node's id in a redundant pair (needs NEXUS_PG_URL)
//   NEXUS_CLUSTER_SECRET  shared secret for node-to-node media fetches
//   NEXUS_LEASE_TTL_MS  governor lease TTL (default 10000)
//   NEXUS_TLS_CERT/KEY  PEM files: serve HTTPS. NEXUS_TLS_CA: accept client certificates it issued (mTLS)
//   NEXUS_OAUTH_ISSUER / NEXUS_OAUTH_JWKS / NEXUS_OAUTH_AUDIENCE  accept OAuth 2 access tokens (JWT)
//   Agent extension (Level 5, see docs/i3X-Nexus-Agent-Extension.html):
//   NEXUS_AGENTS_REQUIRE_GRANT=1  an agent acts only under a grant from a named person
//   NEXUS_SIGNED_APPROVALS=1      every approval is signed with the approver's registered key
//   NEXUS_ANCHOR_KEY    Ed25519 private key (PEM file) that signs the ledger head
//   NEXUS_ANCHOR_FILE / NEXUS_ANCHOR_URL  where anchors go (append-only file, HTTP endpoint)
//   NEXUS_ANCHOR_EVERY_MS  how often the head is anchored when it moved (default 300000)

import { readFileSync } from "node:fs";
import { loadConfig } from "./model/config";
import { Nexus } from "./nexus";
import type { PrincipalDef } from "./core/auth";

const env = process.env;
const config = loadConfig(env.NEXUS_CONFIG ?? "config/example-site.json");
const principalsFile = env.NEXUS_PRINCIPALS;
if (!principalsFile) throw new Error("NEXUS_PRINCIPALS is required: a JSON list of principals (scripts/principal.ts makes one)");
const principals = JSON.parse(readFileSync(principalsFile, "utf8")) as PrincipalDef[];

const nexus = new Nexus({
  config, principals,
  dataDir: env.NEXUS_DATA ?? "data",
  port: Number(env.NEXUS_PORT ?? 8400),
  host: env.NEXUS_HOST ?? "127.0.0.1",
  publicBase: env.NEXUS_PUBLIC_BASE,
  pgUrl: env.NEXUS_PG_URL,
  pgPatroni: env.NEXUS_PG_PATRONI ? env.NEXUS_PG_PATRONI.split(",") : undefined,
  mediaSigningKey: env.NEXUS_MEDIA_KEY,
  tls: env.NEXUS_TLS_CERT && env.NEXUS_TLS_KEY ? {
    cert: readFileSync(env.NEXUS_TLS_CERT, "utf8"), key: readFileSync(env.NEXUS_TLS_KEY, "utf8"),
    ca: env.NEXUS_TLS_CA ? readFileSync(env.NEXUS_TLS_CA, "utf8") : undefined,
  } : undefined,
  oauth: env.NEXUS_OAUTH_ISSUER && env.NEXUS_OAUTH_JWKS && env.NEXUS_OAUTH_AUDIENCE
    ? { issuer: env.NEXUS_OAUTH_ISSUER, jwksUrl: env.NEXUS_OAUTH_JWKS, audience: env.NEXUS_OAUTH_AUDIENCE } : undefined,
  cluster: env.NEXUS_NODE_ID ? {
    nodeId: env.NEXUS_NODE_ID,
    secret: env.NEXUS_CLUSTER_SECRET ?? (() => { throw new Error("NEXUS_CLUSTER_SECRET is required in cluster mode"); })(),
    lease: env.NEXUS_LEASE_TTL_MS ? { ttlMs: Number(env.NEXUS_LEASE_TTL_MS), renewMs: Math.max(200, Number(env.NEXUS_LEASE_TTL_MS) / 5), marginMs: Number(env.NEXUS_LEASE_TTL_MS) / 5 } : undefined,
  } : undefined,
  agents: { requireGrant: env.NEXUS_AGENTS_REQUIRE_GRANT === "1" },
  requireSignedApprovals: env.NEXUS_SIGNED_APPROVALS === "1",
  anchor: env.NEXUS_ANCHOR_KEY ? {
    keyPem: readFileSync(env.NEXUS_ANCHOR_KEY, "utf8"),
    file: env.NEXUS_ANCHOR_FILE, url: env.NEXUS_ANCHOR_URL,
    everyMs: env.NEXUS_ANCHOR_EVERY_MS ? Number(env.NEXUS_ANCHOR_EVERY_MS) : undefined,
  } : undefined,
});

const port = await nexus.start();
console.log(`i3X nexus for ${config.site.name} on :${port}/v1 — ${config.objects.length} objects, ${config.sources.length} sources`);
// A clean stop releases the governor lease at once, so the partner does not wait out
// the TTL. If the database is unreachable that cannot finish: give it 10 s, then go.
for (const sig of ["SIGINT", "SIGTERM"] as const) process.on(sig, () => {
  setTimeout(() => process.exit(0), 10_000).unref();
  void nexus.stop().then(() => process.exit(0), () => process.exit(0));
});
// SIGKILL needs no handler: that is the hard failure the standby must survive.
