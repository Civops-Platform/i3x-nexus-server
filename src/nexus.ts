// One site nexus: the model, the cache, the drivers, the stores and the HTTP
// transport, wired together. `main.ts` runs one from the environment; tests build
// one in-process.
//
// Request flow (spec §4, §8):
//   auth → rate limit → classify → admit to lane → i3X wire / writes / media → release
//
// /info is answered before auth and before admission: it is how a client finds out
// what this server is, and it must answer when everything else is busy.
//
// Cluster mode (profile Level 2, `cluster` + `pgUrl`): two nodes, each with its own
// source sessions and cache, sharing the ledger, subscriptions and media index in
// Postgres. Either node serves every read and subscription. Writes go through the
// governor lease holder; a write that reaches the other node is forwarded to it,
// and while no node holds the lease (failover) writes answer 503 + Retry-After.

import http, { type IncomingMessage, type ServerResponse } from "node:http";
import https from "node:https";
import type { TLSSocket } from "node:tls";
import { JwtVerifier, type OAuthConfig } from "./core/oauth";
import { PatroniPool, parseMembers } from "./cluster/patroni-pool";
import { gzipSync } from "node:zlib";
import { join } from "node:path";
import type { AddressInfo } from "node:net";
import pg from "pg";
import { randomUUID } from "node:crypto";

import type { SiteConfig, SourceDef } from "./model/config";
import { LastValueCache, type CacheChange } from "./core/cache";
import { MemoryHistory, PgHistory, type HistoryStore } from "./core/history";
import { RecordStore, type RecordVersion } from "./core/records";
import { migrate } from "./cluster/schema";
import { GovernorLease, type LeaseOptions, type LeaseState } from "./cluster/lease";
import { expireSubscriptions, readSubscription, withSubscriptions } from "./cluster/subscriptions";
import type { Subscription, SyncUpdate } from "./i3x/wire";
import { MediaStore } from "./core/media";
import { Authenticator, RateLimiter, can, type Principal, type PrincipalDef } from "./core/auth";
import { Lanes, classify, type LaneConfig } from "./core/priority";
import { NexusFabric, shaOfMediaId } from "./i3x/fabric";
import { createSubscriptionStore, handleI3xRequest, handleWrites, ok, problem, type I3xResponse, type VQT } from "./i3x/wire";
import { writeOne } from "./i3x/writes";
import { openStream } from "./i3x/stream";
import type { Driver } from "./drivers/driver";
import { SimulatorDriver } from "./drivers/simulator";
import { MqttDriver } from "./drivers/mqtt";
import { OpcUaDriver } from "./drivers/opcua";
import { SparkplugDriver } from "./drivers/sparkplug";
import { UpstreamMirror } from "./mirror/upstream";
import { Governor, WR_PREFIX, approvalSigningString } from "./governance/governor";
import { AgentControl } from "./agents/control";
import { handleMcp, type Dispatched } from "./agents/mcp";
import { Anchorer, type AnchorOptions } from "./core/anchor";
import { CmmsRestAdapter, type SystemAdapter } from "./governance/adapters";
import { AUTHORITY_SYSTEM, LIMIT_TYPE, LimitAdapter } from "./governance/authority";
import { SystemProjection } from "./governance/projection";
import { SetpointAdapter, envelopeViolation, SETPOINT_SYSTEM, type Envelope } from "./governance/setpoint";
import { IgnitionSqlHistorian, type HistorianAdapter } from "./history/ignition";
import { MediaMtxGateway } from "./media/gateway";
import { MediaMtxDriver } from "./drivers/mediamtx";
import { createHash } from "node:crypto";
import { Readable } from "node:stream";
import type { PriorityClass } from "./model/config";

export interface NexusOptions {
  config: SiteConfig;
  principals: PrincipalDef[];
  dataDir: string;
  port?: number;
  host?: string;
  /** Public base URL including /v1, for signed media links. Defaults to the bound address. */
  publicBase?: string;
  pgUrl?: string;
  /** Patroni REST APIs, one per host in pgUrl: the pool follows the elected primary. */
  pgPatroni?: string[];
  mediaSigningKey?: string;
  rate?: { perSecond: number; burst: number };
  lanes?: Record<PriorityClass, LaneConfig>;
  /** Test hook: pretend the event loop lags by this much (ms). */
  lagOverride?: () => number;
  streamTickMs?: number;
  /** HTTPS (PEM contents). With `ca`, clients may present certificates (mTLS) issued by it. */
  tls?: { cert: string; key: string; ca?: string };
  /** OAuth 2 access tokens (JWT) from the site's identity provider. */
  oauth?: OAuthConfig;
  /** Level 2: run as one node of a redundant pair. Requires pgUrl. */
  cluster?: { nodeId: string; secret: string; lease?: LeaseOptions; heartbeatMs?: number };
  /** Agent extension: agents act only under a grant from a named person (NXA-1). */
  agents?: { requireGrant?: boolean };
  /** Agent extension: every approval is signed with the approver's registered key (NXA-6). */
  requireSignedApprovals?: boolean;
  /** Agent extension: sign and export the ledger head (NXA-7). */
  anchor?: AnchorOptions;
}

const VERSION = "1.1.0";

/** A peer counts as alive while its heartbeat is younger than this. */
const PEER_TIMEOUT_MS = 6_000;

const MAX_BODY = 4 * 1024 * 1024;
const HISTORY_MAX_POINTS = 100_000;

export class Nexus {
  readonly cache = new LastValueCache();
  records: RecordStore;
  lease: GovernorLease | null = null;
  private peers = new Map<string, string>();
  private clusterTimers: NodeJS.Timeout[] = [];
  readonly mirrors = new Map<string, UpstreamMirror>();
  readonly governor: Governor | null = null;
  readonly agents: AgentControl;
  anchorer: Anchorer | null = null;
  readonly projection: SystemProjection | null = null;
  private governorTimer: NodeJS.Timeout | null = null;
  readonly setpoints: SetpointAdapter | null = null;
  private historians = new Map<string, HistorianAdapter>();
  gateway: MediaMtxGateway | null = null;
  /** Clips cut so far (for /info and tests). */
  readonly clipsCut: Array<{ trigger: string; mediaId: string; at: string } | { trigger: string; error: string; at: string }> = [];
  /** attribute id → where its long history lives. */
  private federated = new Map<string, { historian: string; address: string }>();
  readonly media: MediaStore;
  readonly lanes: Lanes;
  readonly fabric: NexusFabric;
  readonly memoryHistory = new MemoryHistory();
  private history: HistoryStore = this.memoryHistory;
  private pool: pg.Pool | null = null;
  private auth: Authenticator;
  private limiter: RateLimiter;
  private subs = createSubscriptionStore();
  private drivers: Driver[] = [];
  private server: http.Server | https.Server | null = null;
  private sweeper: NodeJS.Timeout | null = null;
  private wakers = new Set<(id: string) => void>();
  private base = "";

  constructor(private o: NexusOptions) {
    this.records = new RecordStore(join(o.dataDir, "records.jsonl"));
    this.media = new MediaStore(join(o.dataDir, "media"), o.mediaSigningKey);
    this.lanes = new Lanes(o.lanes, o.lagOverride);
    this.auth = new Authenticator(o.principals, o.oauth ? new JwtVerifier(o.oauth) : null, o.cluster?.secret ?? null);
    this.limiter = new RateLimiter(o.rate?.perSecond ?? 25, o.rate?.burst ?? 120);
    this.agents = new AgentControl({
      records: () => this.records,
      principals: o.principals,
      parentOf: (id) => { const d = this.fabric.objectDef(id); return d ? (d.parentId ?? null) : undefined; },
      requireGrant: o.agents?.requireGrant ?? false,
    });
    this.auth.grants = (t) => this.agents.resolve(t);
    this.fabric = new NexusFabric({
      config: o.config, cache: this.cache, records: this.records, media: this.media, lanes: this.lanes,
      publicBase: () => this.base,
      viewerUrl: (obj) => {
        if (!this.gateway || !obj.stream?.path) return null;
        // Stable within a minute so a subscription does not see a change on every read.
        const bucket = Math.floor(Date.now() / 60_000) * 60_000;
        const { exp, sig } = this.media.sign(`stream:${obj.stream.path}`, "get", 10 * 60_000, bucket);
        return `${this.gateway.cfg.viewerBase}/${obj.stream.path}/?token=${exp}.${sig}`;
      },
      external: { objects: () => this.projection?.objects() ?? [], value: (id) => this.projection?.value(id) ?? null },
      tiers: () => ({ W0: "enabled", W1: this.projection ? "enabled" : "disabled", W2: this.setpoints && !this.w2Suspended() ? "enabled" : this.setpoints ? "suspended" : "disabled" }),
      redundancy: () => this.redundancy(),
      level: () => (this.setpoints ? 4 : this.projection ? 3 : this.o.cluster ? 2 : 1),
    });
    this.media.onIndexChange = () => this.fabric.rebuild();
    this.records.onNewElement = () => this.fabric.rebuild();

    if (o.config.mediaGateway) {
      const g = o.config.mediaGateway;
      const user = process.env[g.userEnv], pass = process.env[g.passEnv];
      if (!user || !pass) throw new Error(`media gateway ${g.id}: set ${g.userEnv} and ${g.passEnv}`);
      this.gateway = new MediaMtxGateway(g, user, pass);
    }
    for (const h of o.config.historians ?? []) {
      const url = process.env[h.urlEnv];
      if (!url) throw new Error(`historian ${h.id}: environment variable ${h.urlEnv} is not set`);
      this.historians.set(h.id, new IgnitionSqlHistorian(h.id, url, h.provider, h.dialect ?? "postgres"));
    }
    for (const obj of o.config.objects) for (const [a, b] of Object.entries(obj.bindings ?? {})) {
      if (b.history) this.federated.set(`${obj.id}/${a}`, b.history);
    }

    // Tier W1: systems of record, their projection, and the governor.
    const systems = (o.config.systems ?? []).map((s) => {
      const token = s.tokenEnv ? process.env[s.tokenEnv] : undefined;
      if (s.tokenEnv && !token) throw new Error(`system ${s.id}: environment variable ${s.tokenEnv} is not set`);
      const adapter: SystemAdapter = new CmmsRestAdapter(s.id, s.url, token);
      return { id: s.id, typeId: s.typeId, name: s.name, adapter };
    });
    // Tier W2: setpoints on W2 types, each attribute with its envelope, written through its source driver.
    const w2 = new Map<string, { binding: NonNullable<SiteConfig["objects"][number]["bindings"]>[string] & { envelope?: Envelope }; object: SiteConfig["objects"][number] }>();
    for (const obj of o.config.objects) {
      if (o.config.types.find((t) => t.id === obj.typeId)?.write !== "W2") continue;
      for (const [a, b] of Object.entries(obj.bindings ?? {})) if (b.envelope) w2.set(`${obj.id}/${a}`, { binding: b, object: obj });
    }
    if (w2.size) {
      this.setpoints = new SetpointAdapter(this.cache, w2, (sourceId) => {
        const d = this.drivers.find((x) => x.source.id === sourceId);
        return d?.write ? { write: (addr, v) => d.write!(addr, v) } : null;
      });
    }
    if (systems.length || this.setpoints) {
      const projection = systems.length ? new SystemProjection(systems) : null;
      if (projection) projection.onStructureChange = () => this.fabric.rebuild();
      this.projection = projection;
      const sp = this.setpoints;
      const isLimit = (id: string) => o.config.objects.some((x) => x.id === id && x.typeId === LIMIT_TYPE);
      this.governor = new Governor({
        records: () => this.records,
        site: o.config.site.id,
        resolveTarget: (id) => (isLimit(id) ? { system: AUTHORITY_SYSTEM, recordId: id } : projection?.resolve(id) ?? null),
        resolveSetpoint: (id, field) => (sp?.has(`${id}/${field}`) ? { system: SETPOINT_SYSTEM, recordId: `${id}/${field}` } : null),
        envelopeCheck: (attr, proposed, current) => envelopeViolation(sp?.envelopeOf(attr) ?? {}, proposed, current),
        w2Suspended: () => this.w2Suspended(),
        adapters: new Map<string, import("./governance/adapters").SystemAdapter>([...systems.map((s) => [s.id, s.adapter] as const), ...(sp ? [[SETPOINT_SYSTEM, sp] as const] : []),
          [AUTHORITY_SYSTEM, new LimitAdapter(() => this.records, isLimit)]]),
        limits: () => o.config.objects.filter((x) => x.typeId === "AuthorityLimit").map((x) => ({ elementId: x.id, value: this.records.current(x.id)?.value })),
        onChange: () => this.fabric.rebuild(),
        agentCheck: (p, tier, target, used) => this.agents.checkRequest(p, tier, target, used),
        agentWritesFrozen: () => this.agents.frozen(),
        requireSignedApprovals: o.requireSignedApprovals ?? false,
      });
    }
    this.cache.on("change", (c: CacheChange) => this.onChange(c));
  }

  private onChange(c: CacheChange) {
    if (!c.changed) return;
    const v = c.current;
    if (v.value !== null) this.history.append([{ attributeId: c.attributeId, value: v.value, quality: v.quality, timestamp: v.timestamp }]);
    if (this.history !== this.memoryHistory && v.value !== null) {
      this.memoryHistory.append([{ attributeId: c.attributeId, value: v.value, quality: v.quality, timestamp: v.timestamp }]);
    }
    const obj = c.attributeId.slice(0, c.attributeId.lastIndexOf("/"));
    // Quality changes are P0 for every element (§8.1): a value going Bad must not wait for a tick.
    if (this.fabric.priorityOf(obj) === "P0" || v.quality !== v.sourceQuality || v.quality === "Bad") {
      for (const w of this.wakers) w(obj);
    }
  }

  // ── lifecycle ───────────────────────────────────────────────────────────

  async start() {
    if (this.o.cluster && !this.o.pgUrl) throw new Error("cluster mode needs pgUrl: the pair shares its ledger, subscriptions and media index in Postgres");
    if (this.o.pgUrl) {
      if (this.o.pgPatroni?.length) {
        const { url, members } = parseMembers(this.o.pgUrl, this.o.pgPatroni);
        const pp = new PatroniPool(url, members, { max: 12 });
        pp.on("error", () => { /* a dropped connection to a demoted primary; the next call re-finds the leader */ });
        await pp.find();
        this.pool = pp as unknown as pg.Pool;
      } else {
        this.pool = new pg.Pool({ connectionString: this.o.pgUrl, max: 12 });
      }
      if (this.o.cluster) await migrate(this.pool);
      this.history = await PgHistory.open(this.pool);
    }
    // Listen first: the lease and the peers need this node's URL.
    const handler = (req: IncomingMessage, res: ServerResponse) => { void this.handle(req, res); };
    this.server = this.o.tls
      // requestCert without rejectUnauthorized: a client certificate is optional (GET /info and
      // bearer callers need none) but, when presented, only one the CA issued identifies anyone.
      ? https.createServer({ cert: this.o.tls.cert, key: this.o.tls.key, ca: this.o.tls.ca, requestCert: !!this.o.tls.ca, rejectUnauthorized: false, minVersion: "TLSv1.2" }, handler)
      : http.createServer(handler);
    await new Promise<void>((r) => this.server!.listen(this.o.port ?? 8400, this.o.host ?? "127.0.0.1", () => r()));
    const a = this.server.address() as AddressInfo;
    this.base = this.o.publicBase ?? `${this.o.tls ? "https" : "http"}://${a.address}:${a.port}/v1`;

    if (this.o.cluster) await this.startCluster();
    else await this.seedInitial();
    if (this.gateway) {
      for (const obj of this.o.config.objects) {
        if (obj.stream?.path && obj.stream.cameraUrl) await this.gateway.ensurePath(obj.stream.path, obj.stream.cameraUrl);
      }
      this.armClips();
    }
    await this.startDrivers();
    if (this.projection) await this.projection.start();
    if (this.governor) {
      // Only the governor finishes interrupted requests and expires stale ones (NX-51).
      const gov = this.governor;
      const tick = async () => { if (!this.lease || this.lease.isLeader()) await gov.resume().catch(() => {}); };
      this.lease?.on("acquired", () => void tick());
      this.governorTimer = setInterval(() => void tick(), 5_000);
    }
    for (const u of this.o.config.upstreams ?? []) {
      const token = process.env[u.tokenEnv];
      if (!token) throw new Error(`upstream ${u.id}: environment variable ${u.tokenEnv} is not set`);
      const m = new UpstreamMirror({ id: u.id, url: u.url, token }, this.fabric, this.cache, () => this.history);
      this.mirrors.set(u.id, m);
      await m.start();
    }
    if (this.o.anchor) {
      this.anchorer = new Anchorer(this.o.anchor, this.o.config.site.id, this.o.cluster?.nodeId ?? "single",
        () => this.records.head(), () => !this.lease || this.lease.isLeader());
      this.anchorer.start();
    }
    this.sweeper = setInterval(() => this.cache.sweep(), 1000);
    return a.port;
  }

  private async seedInitial() {
    for (const obj of this.o.config.objects) {
      if (obj.initial && !this.records.current(obj.id)) {
        await this.records.write(obj.id, obj.initial, "nexus:config", undefined, `nexus:config:${obj.id}:initial`).catch(() => {});
      }
    }
  }

  private async startCluster() {
    const c = this.o.cluster!;
    const pool = this.pool!;
    this.lease = new GovernorLease(pool, c.nodeId, () => this.base, c.lease);
    this.records = await RecordStore.postgres(pool, () => (this.lease!.isLeader() ? this.lease!.epoch() : null), c.nodeId);
    this.records.onNewElement = () => this.fabric.rebuild();
    this.fabric.useRecords(this.records);
    await this.media.attachCluster({ pool, nodeId: c.nodeId, secret: c.secret, peerUrl: (id) => this.peers.get(id) ?? null });
    const beat = async () => {
      await pool.query(
        `INSERT INTO nexus_nodes (node_id, url, heartbeat, started_at) VALUES ($1, $2, now(), now())
         ON CONFLICT (node_id) DO UPDATE SET url = EXCLUDED.url, heartbeat = now()`, [c.nodeId, this.base]);
      const r = await pool.query(`SELECT node_id, url FROM nexus_nodes WHERE heartbeat > now() - ($1 || ' milliseconds')::interval`, [String(PEER_TIMEOUT_MS)]);
      this.peers = new Map(r.rows.map((x) => [x.node_id as string, x.url as string]));
    };
    await beat().catch(() => {});
    this.clusterTimers.push(setInterval(() => void beat().catch(() => { this.peers = new Map(); }), c.heartbeatMs ?? 2_000));
    this.clusterTimers.push(setInterval(() => void expireSubscriptions(pool).catch(() => {}), 60_000));
    this.lease.on("acquired", () => void this.seedInitial());
    this.lease.start();
  }

  private async authenticate(req: IncomingMessage) {
    const sock = req.socket as TLSSocket;
    let certSubject: string | null = null;
    if (sock.encrypted && sock.authorized) {
      const c = sock.getPeerCertificate();
      certSubject = c && c.subject ? String((c.subject as unknown as Record<string, string>).CN ?? "") || null : null;
    }
    const fwd = req.headers["x-nexus-forwarded-principal"];
    return this.auth.fromRequest({ authorization: req.headers.authorization, certSubject, forwardedPrincipal: typeof fwd === "string" ? fwd : undefined });
  }

  /** NX-53: in a pair, setpoint writes stop while redundancy is degraded. */
  w2Suspended(): string | null {
    if (!this.o.cluster) return null;
    return this.redundancy().state === "ok" ? null : "redundancy is degraded (fewer than two live nodes)";
  }

  /** Profile Annex B `redundancy`. Degraded when fewer than two nodes are alive (NX-53). */
  redundancy() {
    if (!this.o.cluster) return { mode: "single", state: "ok", endpoints: [this.base] };
    const alive = [...this.peers.values()];
    return {
      mode: "redundant",
      state: alive.length >= 2 ? "ok" : "degraded",
      node: this.o.cluster.nodeId,
      role: this.lease?.isLeader() ? "governor" : "standby",
      governor: this.lease?.state.holder ?? null,
      endpoints: alive.length ? alive : [this.base],
    };
  }

  /** Test hook: cut this node off from the shared database (lease, ledger, heartbeats). */
  partition(on: boolean) { if (this.lease) this.lease.partitioned = on; }

  async stop(opts: { graceful?: boolean } = { graceful: true }) {
    if (this.sweeper) clearInterval(this.sweeper);
    this.anchorer?.stop();
    for (const t of this.clusterTimers) clearInterval(t);
    for (const m of this.mirrors.values()) m.stop();
    this.projection?.stop();
    for (const h of this.historians.values()) await h.close().catch(() => {});
    if (this.governorTimer) clearInterval(this.governorTimer);
    this.lease?.stop();
    if (opts.graceful !== false) await this.lease?.release();
    this.media.detach();
    await this.records.close();
    await Promise.all(this.drivers.map((d) => d.stop()));
    await new Promise<void>((r) => (this.server ? this.server.close(() => r()) : r()));
    this.server?.closeAllConnections?.();
    await this.history.close?.();
    await this.pool?.end();
    this.lanes.close();
  }

  driverStatus() {
    return this.drivers.map((d) => ({ source: d.source.id, driver: d.source.driver, ...d.status() }));
  }

  private async startDrivers() {
    for (const s of this.o.config.sources) {
      const points: Array<{ attributeId: string; binding: NonNullable<SiteConfig["objects"][number]["bindings"]>[string] }> = [];
      for (const obj of this.o.config.objects) {
        for (const [attr, b] of Object.entries(obj.bindings ?? {})) {
          if (b.source !== s.id) continue;
          const id = `${obj.id}/${attr}`;
          // Staleness only where an update period is declared: report-by-exception
          // sources (OPC UA subscriptions, Sparkplug, most MQTT) are silent when steady.
          this.cache.declare(id, s.id, b.scanMs ?? (s.driver === "simulator" ? 1000 : 0));
          points.push({ attributeId: id, binding: b });
        }
      }
      gatewayRef = this.gateway;
      const d = makeDriver(s);
      this.drivers.push(d);
      await d.start(points, {
        value: (id, v, q, ts) => this.cache.set(id, v, q, ts),
        lost: () => this.cache.sourceLost(s.id),
        degrade: (id, q) => this.cache.mark(id, q),
        restored: () => { /* values return as the source sends them; nothing to fabricate */ },
      });
    }
  }

  // ── HTTP ────────────────────────────────────────────────────────────────

  private async handle(req: IncomingMessage, res: ServerResponse) {
    try {
      const url = new URL(req.url ?? "/", "http://nexus");
      const path = (url.pathname.replace(/^\/v1(?=\/|$)/, "") || "/").replace(/\/+$/, "") || "/";
      const method = (req.method ?? "GET").toUpperCase();
      const query: Record<string, string> = {};
      url.searchParams.forEach((v, k) => { query[k] = v; });

      // Media bytes carry their own signed authority; no bearer token, no JSON body.
      if (path.startsWith("/x-nexus/media/")) return await this.mediaRoute(req, res, method, path, url);
      if (path.startsWith("/x-nexus/peer/media/")) return await this.peerMediaRoute(req, res, path);
      if (path === "/x-nexus/gateway/auth" && method === "POST") return await this.gatewayAuth(req, res);

      if (method === "GET" && path === "/info") return this.send(req, res, handleI3xRequest({ method: "GET", path, authenticated: false }, this.fabric, this.subs));

      const p = await this.authenticate(req);
      if (!p || !can(p, "read")) return this.send(req, res, problem(401, "Unauthorized", "i3X requires a bearer token with the read scope for every endpoint except GET /info."));
      const gate = this.agents.admit(p);
      if (gate) return this.send(req, res, problem(gate.status, "Forbidden", gate.detail));
      (req as Scoped).nexusPrincipal = p;
      const rl = this.limiter.take(p.id);
      if (!rl.ok) return this.send(req, res, problem(429, "Too Many Requests", `rate limit for ${p.id}; retry in ${rl.retryAfterS} s`), { "Retry-After": String(rl.retryAfterS) });

      let body: Record<string, unknown> | undefined;
      if (method === "POST" || method === "PUT") {
        const raw = await readBody(req, MAX_BODY);
        if (raw === null) return this.send(req, res, problem(413, "Payload Too Large", `request bodies are limited to ${MAX_BODY} bytes`));
        try { body = raw.length ? JSON.parse(raw.toString("utf8")) : undefined; } catch { return this.send(req, res, problem(400, "Bad Request", "body is not valid JSON")); }
      }

      if (path === "/mcp") return await this.mcpRoute(req, res, method, body, p);
      await this.serve(req, res, method as "GET" | "POST" | "PUT", path, query, body, p);
    } catch (e) {
      if (!res.headersSent) this.send(req, res, problem(500, "Internal Error", (e as Error).message));
      else res.end();
    }
  }

  /** Everything after authentication: an agent's areas, the priority lanes, then the route. */
  private async serve(req: IncomingMessage, res: ServerResponse, method: "GET" | "POST" | "PUT", path: string,
    query: Record<string, string>, body: Record<string, unknown> | undefined, p: Principal): Promise<void> {
    if (p.grant) {
      const named = [...namedElements(body), ...[query.elementId, query.parentId].filter((x): x is string => !!x)];
      const outside = named.filter((id) => !this.agentSees(p, id));
      if (outside.length) {
        this.agents.flag(p.id, p.grant.id, `asked for elements outside its grant: ${outside.slice(0, 5).join(", ")}`);
        return this.send(req, res, problem(403, "Outside grant", `outside grant ${p.grant.id}'s areas (${p.grant.areas.join(", ")}): ${outside.join(", ")}`));
      }
    }
    const cls = classify(method, path, namedElements(body).map((id) => this.fabric.priorityOf(id)));
    const admit = await this.lanes.admit(cls);
    if (!admit.ok) {
      return this.send(req, res, problem(cls === "P3" ? 503 : 429, "Busy", admit.reason), { "Retry-After": String(admit.retryAfterS) });
    }
    try {
      await this.route(req, res, method, path, query, body, p);
    } finally {
      admit.release();
    }
  }

  /** An agent on a grant sees its areas, the intakes, and its own write requests. */
  private agentSees(p: Principal, id: string) {
    if (id.startsWith(WR_PREFIX)) return this.governor?.current(id)?.actor.principal === p.id;
    return this.agents.inArea(p, id);
  }

  /** NXA-4: MCP over HTTP. Each tool call runs through serve() as the caller. */
  private async mcpRoute(req: IncomingMessage, res: ServerResponse, method: string, body: unknown, p: Principal) {
    if (method !== "POST") { res.writeHead(405, { Allow: "POST" }); res.end(); return; }
    const dispatch = async (m: "GET" | "POST" | "PUT", path: string, query: Record<string, string>, b: unknown, headers: Record<string, string> = {}): Promise<Dispatched> => {
      const inner = Object.assign(Object.create(req), {
        headers: { ...(req.headers.authorization ? { authorization: req.headers.authorization } : {}), ...headers },
        nexusPrincipal: p,
      }) as IncomingMessage;
      const cap = { status: 0, headers: {} as Record<string, string>, chunks: [] as Buffer[], headersSent: false };
      const fake = {
        get headersSent() { return cap.headersSent; },
        writeHead(status: number, h: Record<string, string> = {}) { cap.status = status; cap.headersSent = true; for (const [k, v] of Object.entries(h)) cap.headers[k.toLowerCase()] = String(v); return fake; },
        write(c: Buffer | string) { cap.chunks.push(Buffer.from(c)); return true; },
        end(c?: Buffer | string) { if (c) cap.chunks.push(Buffer.from(c)); },
        on() { return fake; }, once() { return fake; },
      } as unknown as ServerResponse;
      await this.serve(inner, fake, m, path, query, b as Record<string, unknown> | undefined, p);
      const text = Buffer.concat(cap.chunks).toString("utf8");
      let parsed: unknown = text;
      try { parsed = text ? JSON.parse(text) : null; } catch { /* keep text */ }
      return { status: cap.status, headers: cap.headers, body: parsed };
    };
    const out = await handleMcp(body, dispatch, VERSION);
    if (out === null) { res.writeHead(202); res.end(); return; }
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(JSON.stringify(out));
  }

  /** Agent extension administration (§3) and approval signing info (§5). */
  private async agentsRoute(req: IncomingMessage, res: ServerResponse, method: string, path: string, body: Record<string, unknown> | undefined, p: Principal): Promise<void> {
    const send = (r: { ok: true; value: unknown } | { ok: false; status: number; detail: string }) =>
      this.send(req, res, r.ok ? ok(r.value) : problem(r.status, r.status === 404 ? "Not Found" : r.status === 400 ? "Bad Request" : r.status === 409 ? "Conflict" : "Forbidden", r.detail));
    const wrs = () => (this.governor?.ids() ?? []).map((i) => this.governor!.current(i)!).filter(Boolean);
    if (method === "GET" && path === "/x-nexus/agents") {
      const reg = this.agents.registry(p, wrs());
      return send(reg ? { ok: true, value: reg } : { ok: false, status: 403, detail: "the registry needs agents:admin, or agents:grant for the grants you sponsor" });
    }
    if (method === "GET" && path === "/x-nexus/agents/me") {
      if (p.kind !== "agent") return send({ ok: false, status: 403, detail: "only an agent has a grant" });
      const g = p.grant;
      const used = g ? wrs().filter((r) => r.actor.grant?.id === g.id).length : null;
      return send({ ok: true, value: { agent: p.id, grant: g ? { ...g, requestsUsed: used, requestsLeft: Math.max(0, g.maxRequests - (used ?? 0)) } : null, writesFrozen: this.agents.frozen() } });
    }
    if (method === "GET" && path.startsWith("/x-nexus/requests/")) {
      const id = decodeURIComponent(path.slice("/x-nexus/requests/".length));
      const v = this.records.current(id);
      if (!v || !id.startsWith(WR_PREFIX)) return send({ ok: false, status: 404, detail: `no WriteRequest ${id}` });
      return send({ ok: true, value: { id, versionHash: v.hash, sign: { approve: approvalSigningString(id, "approve", v.hash), reject: approvalSigningString(id, "reject", v.hash) } } });
    }
    if (method !== "PUT") return send({ ok: false, status: 404, detail: `${method} ${path} is not a nexus agent endpoint` });
    if (this.lease && !this.lease.isLeader()) return this.forwardWrite(req, res, path, body);
    const seg = path.slice("/x-nexus/agents/".length).split("/").map(decodeURIComponent);
    if (seg[0] === "grants" && seg.length === 1) return send(await this.agents.issue(p, body));
    if (seg[0] === "grants" && seg[2] === "revoke") return send(await this.agents.revoke(p, seg[1], body));
    if (seg[0] === "freeze" && seg.length === 1) return send(await this.agents.setFrozen(p, true, body));
    if (seg[0] === "release" && seg.length === 1) return send(await this.agents.setFrozen(p, false, body));
    if (seg.length === 2 && (seg[1] === "suspend" || seg[1] === "resume")) return send(await this.agents.setSuspended(p, seg[0], seg[1] === "suspend"));
    return send({ ok: false, status: 404, detail: `PUT ${path} is not a nexus agent endpoint` });
  }

  private async route(req: IncomingMessage, res: ServerResponse, method: "GET" | "POST" | "PUT", path: string,
    query: Record<string, string>, body: Record<string, unknown> | undefined, p: Principal): Promise<void> {
    if (path.startsWith("/x-nexus/agents") || path.startsWith("/x-nexus/requests/")) return this.agentsRoute(req, res, method, path, body, p);
    if (method === "PUT" && (path === "/objects/value" || path === "/objects/history")) {
      // A batch for a mounted site goes to that site whole (NX-48); the site decides it.
      const ups = new Set(namedElements(body).map((id) => this.fabric.mountOf(id)));
      if (ups.size === 1 && !ups.has(null)) {
        const m = this.mirrors.get([...ups][0]!)!;
        try {
          const r = await m.forward(path, body, req.headers.authorization, typeof req.headers["idempotency-key"] === "string" ? req.headers["idempotency-key"] : undefined);
          res.writeHead(r.status, { "Content-Type": "application/json", ...(r.written ? { "X-Nexus-Written": r.written } : {}) });
          res.end(r.text);
        } catch (e) {
          this.send(req, res, problem(503, "Site unreachable", `the owning site nexus did not answer: ${(e as Error).message}`), { "Retry-After": "5" });
        }
        return;
      }
      if (this.lease && !this.lease.isLeader()) return this.forwardWrite(req, res, path, body);
      const mode = path === "/objects/value" ? "current" : "history";
      const key = typeof req.headers["idempotency-key"] === "string" ? req.headers["idempotency-key"] : undefined;
      // i3X 1.0 write results carry no payload, so the ids of what was created (a
      // WriteRequest, a MediaItem, a record version) are returned in a header.
      const written: Array<string | null> = [];
      const out = await handleWrites(body, (id) => !!this.fabric.objectDef(id), async (u, id, i) => {
        const r = await writeOne({ fabric: this.fabric, records: this.records, media: this.media, governor: this.governor, idempotencyKey: key ? `${key}:${i}` : undefined }, p, u, id, mode);
        written[i] = r.ok ? (r.id ?? null) : null;
        if (r.ok && this.governor && this.projection) {
          const wr = this.governor.current(r.id ?? "");
          if (wr?.target.recordId) void this.projection.refresh(wr.target.system, wr.target.recordId).catch(() => {});
        }
        return r;
      });
      return this.send(req, res, out, { "X-Nexus-Written": JSON.stringify(written) });
    }

    if (this.o.cluster && path.startsWith("/subscriptions")) return this.clusterSubscriptions(req, res, method, path, query, body);

    if (method === "POST" && path === "/objects/history") {
      const ids = Array.isArray(body?.elementIds) ? (body!.elementIds as unknown[]).filter((x): x is string => typeof x === "string") : [];
      const from = body?.startTime, to = body?.endTime;
      if (ids.length && typeof from === "string" && typeof to === "string" && !Number.isNaN(Date.parse(from)) && !Number.isNaN(Date.parse(to))) {
        const attrs = this.fabric.realtimeAttributeIds(ids);
        let pts: VQT[] = [];
        try {
          // Federated attributes come from the plant historian; the rest from the nexus's own store.
          const own = attrs.filter((x) => !this.federated.has(x));
          const byHistorian = new Map<string, Array<{ attributeId: string; address: string }>>();
          for (const x of attrs) {
            const f = this.federated.get(x);
            if (f) byHistorian.set(f.historian, [...(byHistorian.get(f.historian) ?? []), { attributeId: x, address: f.address }]);
          }
          const parts = await Promise.all([
            own.length ? this.history.read(own, from, to, HISTORY_MAX_POINTS) : Promise.resolve([]),
            ...[...byHistorian].map(([h, m]) => this.historians.get(h)!.read(m, from, to, HISTORY_MAX_POINTS).catch((e) => {
              throw new Error(`historian ${h}: ${(e as Error).message}`);
            })),
          ]);
          pts = parts.flat();
        } catch (e) {
          // A failed history store is a failed request, never an empty series that reads as "no data".
          return this.send(req, res, problem(503, "History unavailable", `the history store did not answer: ${(e as Error).message}`), { "Retry-After": "5" });
        }
        this.fabric.prefetched = pts;
        try { return this.send(req, res, handleI3xRequest({ method, path, authenticated: true, query, body }, this.fabric, this.subs)); }
        finally { this.fabric.prefetched = null; }
      }
    }

    const out = handleI3xRequest({ method, path, authenticated: true, query, body }, this.fabric, this.subs);
    if (method === "POST" && path === "/subscriptions/stream" && out.status === 200) {
      const r = (out.body as { result: { subscriptionId: string; generation: number; pending: SyncUpdate[]; lastSeen: Array<[string, string]> } }).result;
      return this.openStreamFor(req, res, body, r, () => this.subs.subs.get(r.subscriptionId) ?? null);
    }
    return this.send(req, res, out);
  }

  /**
   * A write that reached the standby goes to the governor, with the caller's own
   * credential, so the governor authenticates and authorises the caller itself.
   * One hop only: a forwarded write that lands on a non-leader is refused.
   */
  private async forwardWrite(req: IncomingMessage, res: ServerResponse, path: string, body: unknown): Promise<void> {
    const busy = (why: string) => this.send(req, res, problem(503, "Write leader unavailable", why), { "Retry-After": "2" });
    if (req.headers["x-nexus-forwarded"]) return busy("this node does not hold the governor lease");
    const me = this.o.cluster!.nodeId;
    const usable = (s: LeaseState) => !!s.holder && s.holder !== me && s.expiresAt > Date.now() && !!s.holderUrl;
    // During a takeover there is a window with no governor, or with a governor that is
    // dead but whose lease has not expired yet (at most the lease TTL plus a renewal or
    // two). Hold the write through it: keep re-reading the lease, take the write locally
    // the moment this node governs, and forward once a live governor exists.
    const waitUntil = Date.now() + (this.o.cluster!.lease?.ttlMs ?? 10_000) + 5_000;
    let holder = this.lease!.state;
    let lastError = "";
    // Every forwarded batch carries an idempotency key; one is minted if the client sent
    // none. A governor that dies mid-request leaves an unknown outcome, and the shared
    // ledger's unique key is what makes re-sending it to the next governor safe.
    const key = typeof req.headers["idempotency-key"] === "string" ? req.headers["idempotency-key"] : `fwd-${randomUUID()}`;
    const caller = await this.authenticate(req);
    for (;;) {
      if (this.lease!.isLeader()) return this.route(req, res, "PUT", path, {}, body as Record<string, unknown>, (await this.authenticate(req))!);
      if (usable(holder)) {
        try {
          const r = await fetch(holder.holderUrl + path, {
            method: "PUT",
            headers: {
              "content-type": "application/json", "x-nexus-forwarded": me,
              // The caller was authenticated here; mTLS identity does not travel, so vouch for it.
              ...(caller ? { "x-nexus-forwarded-principal": this.auth.assertForwarded(caller.id) } : {}),
              ...(req.headers.authorization ? { authorization: req.headers.authorization } : {}),
              "idempotency-key": key,
            },
            body: JSON.stringify(body ?? {}),
            signal: AbortSignal.timeout(10_000),
          });
          const text = await r.text();
          // The governor lost the lease between our read and its write: go round again.
          if (r.status === 503 && /does not hold the governor lease|lost the governor lease/.test(text) && Date.now() < waitUntil) {
            lastError = "governor changed";
          } else {
            res.writeHead(r.status, {
              "Content-Type": "application/json",
              ...(r.headers.get("retry-after") ? { "Retry-After": r.headers.get("retry-after")! } : {}),
              ...(r.headers.get("x-nexus-written") ? { "X-Nexus-Written": r.headers.get("x-nexus-written")! } : {}),
            });
            res.end(text);
            return;
          }
        } catch (e) {
          // Unreachable, or died mid-request. Re-sending with the same key applies it at most once.
          const code = ((e as { cause?: { code?: string } }).cause?.code) ?? (e as Error).name;
          lastError = `governor ${holder.holder} did not answer (${code})`;
        }
      }
      if (Date.now() >= waitUntil) {
        return busy(`no live node holds the governor lease (failover in progress; ${lastError || `last seen holder ${holder.holder ?? "none"}`})`);
      }
      await new Promise((r) => setTimeout(r, 200));
      holder = await this.lease!.refresh().catch(() => holder);
    }
  }

  private async clusterSubscriptions(req: IncomingMessage, res: ServerResponse, method: "GET" | "POST" | "PUT", path: string,
    query: Record<string, string>, body: Record<string, unknown> | undefined) {
    const pool = this.pool!;
    const ids = [
      ...(typeof body?.subscriptionId === "string" ? [body.subscriptionId] : []),
      ...(Array.isArray(body?.subscriptionIds) ? (body!.subscriptionIds as unknown[]).filter((x): x is string => typeof x === "string") : []),
    ];
    const { result: out, tooMany } = await withSubscriptions(pool, ids,
      (store) => handleI3xRequest({ method, path, authenticated: true, query, body }, this.fabric, store),
      { creating: path === "/subscriptions" });
    if (tooMany) return this.send(req, res, problem(429, "Too Many Subscriptions", "this nexus holds its maximum number of subscriptions; delete unused ones first"));
    if (method === "POST" && path === "/subscriptions/stream" && out.status === 200) {
      const r = (out.body as { result: { subscriptionId: string; generation: number; pending: SyncUpdate[]; lastSeen: Array<[string, string]> } }).result;
      // The stream checks the shared row once a second: a newer stream on either node closes this one.
      let current: Subscription | null = await readSubscription(pool, r.subscriptionId);
      const poll = setInterval(() => void readSubscription(pool, r.subscriptionId).then((s) => { current = s; }).catch(() => {}), 1_000);
      res.on("close", () => clearInterval(poll));
      return this.openStreamFor(req, res, body, r, () => current);
    }
    return this.send(req, res, out);
  }

  /** P0 events since the client's Last-Event-ID, from shared history, for elements it monitors. */
  private async replaySince(req: IncomingMessage, monitored: Iterable<string>): Promise<SyncUpdate[]> {
    const since = req.headers["last-event-id"];
    if (typeof since !== "string" || Number.isNaN(Date.parse(since))) return [];
    const p0 = [...monitored].filter((id) => this.fabric.priorityOf(id) === "P0" && this.fabric.typeOf(id)?.attributes.length === 1);
    if (!p0.length) return [];
    const attrs = this.fabric.realtimeAttributeIds(p0);
    const pts = await this.history.read(attrs, new Date(Date.parse(since) + 1).toISOString(), new Date().toISOString(), 10_000).catch(() => []);
    return pts.sort((a, b) => a.timestamp.localeCompare(b.timestamp))
      .map((v) => ({ elementId: v.attributeId.slice(0, v.attributeId.lastIndexOf("/")), value: v.value, quality: v.quality, timestamp: v.timestamp }));
  }

  private async openStreamFor(req: IncomingMessage, res: ServerResponse, body: Record<string, unknown> | undefined,
    r: { subscriptionId: string; generation: number; pending: SyncUpdate[]; lastSeen: Array<[string, string]> }, sub: () => Subscription | null) {
    const s0 = sub();
    const replay = s0 ? await this.replaySince(req, s0.monitored.keys()) : [];
    openStream({
      res, clientId: String(body?.clientId), generation: r.generation, pending: r.pending, lastSeen: r.lastSeen, replay,
      sub,
      fabric: this.fabric,
      isP0: (id) => this.fabric.priorityOf(id) === "P0",
      onUrgent: (w) => { this.wakers.add(w); return () => this.wakers.delete(w); },
      tickMs: this.o.streamTickMs,
    });
  }

  /**
   * The media gateway asks about every viewer, publisher and API caller. Viewers
   * need the signed token from the stream's viewerUrl; the nexus itself uses the
   * gateway credentials; nobody publishes (devices are pulled).
   */
  private async gatewayAuth(req: IncomingMessage, res: ServerResponse) {
    const deny = (why: string) => { res.writeHead(401, { "content-type": "text/plain" }); res.end(why); };
    if (!this.gateway) return deny("no gateway");
    const raw = await readBody(req, 64 * 1024);
    let a: { user?: string; password?: string; action?: string; path?: string; query?: string };
    try { a = JSON.parse(raw?.toString() ?? "{}"); } catch { return deny("bad request"); }
    if (a.user === this.gateway.user && a.password === this.gateway.pass && ["api", "playback", "read", "metrics"].includes(a.action ?? "")) {
      res.writeHead(200).end(); return;
    }
    if (a.action === "read") {
      const tok = new URLSearchParams(a.query ?? "").get("token") ?? "";
      const [exp, sig] = tok.split(".");
      if (a.path && this.media.verify(`stream:${a.path}`, "get", exp ?? null, sig ?? null)) { res.writeHead(200).end(); return; }
      return deny("a viewer needs the signed token from the stream's viewerUrl");
    }
    return deny(`${a.action ?? "unknown"} is not allowed: devices are pulled by the gateway, never pushed to`);
  }

  /** Alarm → clip around it → MediaItem that Documents the alarm (profile §6). */
  private armClips() {
    const rules = this.o.config.clips ?? [];
    if (!rules.length) return;
    this.cache.on("change", (c: CacheChange) => {
      if (!c.changed || c.current.quality !== "Good") return;
      const obj = c.attributeId.slice(0, c.attributeId.lastIndexOf("/"));
      const v = c.current.value;
      const tripped = v === true || (typeof v === "string" && /trip|active|alarm|on/i.test(v) && !/clear|off|inactive/i.test(v));
      if (!tripped) return;
      for (const r of rules.filter((x) => x.trigger === obj)) {
        const at = new Date(Date.parse(c.current.timestamp) || Date.now());
        setTimeout(() => void this.cutClip(r, at), (r.postS + 2) * 1000).unref();
      }
    });
  }

  async cutClip(r: { trigger: string; stream: string; preS: number; postS: number }, at: Date) {
    const stamp = new Date().toISOString();
    try {
      const s = this.o.config.objects.find((x) => x.id === r.stream)?.stream;
      if (!s?.path || !this.gateway) throw new Error(`${r.stream} has no gateway path`);
      // The gateway serves a recording only once its segment has closed: retry for up to a minute.
      let bytes: Buffer | null = null;
      for (let i = 0; ; i++) {
        try { bytes = await this.gateway.clip(s.path, new Date(at.getTime() - r.preS * 1000), r.preS + r.postS); break; }
        catch (e) { if (i >= 11 || !/HTTP 404/.test((e as Error).message)) throw e; await new Promise((x) => setTimeout(x, 5_000)); }
      }
      if (!bytes) throw new Error("no clip");
      const sha = createHash("sha256").update(bytes).digest("hex");
      await this.media.request({ sha256: sha, mimeType: "video/mp4", size: bytes.length, capturedBy: r.stream, documents: r.trigger,
        capturedAt: at.toISOString(), durationMs: (r.preS + r.postS) * 1000, description: `Clip around ${r.trigger}` }, "nexus:clipper");
      const ok = await this.media.receive(sha, Readable.from([bytes]));
      if (!ok.ok) throw new Error(ok.detail);
      this.fabric.rebuild();
      this.clipsCut.push({ trigger: r.trigger, mediaId: `nexus:media:${sha}`, at: stamp });
    } catch (e) {
      this.clipsCut.push({ trigger: r.trigger, error: (e as Error).message, at: stamp });
    }
  }

  /** Node-to-node byte fetch for media replication; authenticated by the cluster secret. */
  private async peerMediaRoute(req: IncomingMessage, res: ServerResponse, path: string) {
    const sha = path.slice("/x-nexus/peer/media/".length);
    const exp = req.headers["x-nexus-peer-exp"], sig = req.headers["x-nexus-peer-sig"];
    if (!/^[0-9a-f]{64}$/.test(sha) || !this.media.verifyPeer(sha, exp as string, sig as string)) {
      return this.send(req, res, problem(403, "Forbidden", "peer media fetch needs a valid cluster signature"));
    }
    if (!this.media.hasLocal(sha)) return this.send(req, res, problem(404, "Not Found", "this node does not hold those bytes"));
    const { createReadStream } = await import("node:fs");
    res.writeHead(200, { "Content-Type": "application/octet-stream" });
    await new Promise<void>((resolve) => createReadStream(this.media.path(sha)).on("end", resolve).on("error", resolve).pipe(res));
  }

  private async mediaRoute(req: IncomingMessage, res: ServerResponse, method: string, path: string, url: URL) {
    const sha = path.slice("/x-nexus/media/".length);
    const item = /^[0-9a-f]{64}$/.test(sha) ? this.media.get(sha) : undefined;
    const exp = url.searchParams.get("exp"), sig = url.searchParams.get("sig");
    const verb = method === "PUT" ? "put" : "get";
    if (!item) return this.send(req, res, problem(404, "Not Found", "no such media item"));
    if (!this.media.verify(sha, verb, exp, sig)) return this.send(req, res, problem(403, "Forbidden", "the signed URL is missing, wrong or expired; read the item's href again"));
    const admit = await this.lanes.admit("P3");
    if (!admit.ok) return this.send(req, res, problem(503, "Busy", admit.reason), { "Retry-After": String(admit.retryAfterS) });
    try {
      if (method === "PUT") {
        const r = await this.media.receive(sha, req);
        if (!r.ok) return this.send(req, res, problem(r.status, "Upload rejected", r.detail));
        res.writeHead(204).end();
        return;
      }
      if (method !== "GET") return this.send(req, res, problem(405, "Method Not Allowed", "GET or PUT"));
      if (item.status !== "available") return this.send(req, res, problem(409, "Not yet uploaded", "the bytes for this item have not arrived"));
      if (!this.media.hasLocal(sha)) {
        // Not replicated here yet: stream it from the node that holds it.
        const peer = await this.media.peerStream(sha);
        if (!peer) return this.send(req, res, problem(503, "Media unavailable", "no live node holds these bytes yet"), { "Retry-After": "5" });
        res.writeHead(200, { "Content-Type": item.mimeType, "Content-Length": String(item.size), "Cache-Control": "private, max-age=60" });
        await new Promise<void>((resolve) => peer.on("end", resolve).on("error", resolve).pipe(res));
        return;
      }
      const { createReadStream } = await import("node:fs");
      res.writeHead(200, { "Content-Type": item.mimeType, "Content-Length": String(item.size), "Cache-Control": "private, max-age=60" });
      await new Promise<void>((resolve) => createReadStream(this.media.path(sha)).on("end", resolve).on("error", resolve).pipe(res));
    } finally {
      admit.release();
    }
  }

  /** A listing never shows an agent on a grant what lies outside its areas (NXA-3). */
  private filterForGrant(p: Principal, out: I3xResponse): I3xResponse {
    const b = out.body as { result?: unknown; results?: unknown };
    const sees = (x: unknown) => {
      const id = (x as { elementId?: unknown })?.elementId;
      return typeof id !== "string" || this.agentSees(p, id);
    };
    if (Array.isArray(b?.result)) return { ...out, body: { ...b, result: b.result.filter(sees) } };
    if (Array.isArray(b?.results)) {
      return { ...out, body: { ...b, results: b.results.map((x) => sees(x) ? x
        : { success: false, elementId: (x as { elementId: string }).elementId, responseDetail: { title: "Outside grant", status: 403, detail: `outside grant ${p.grant!.id}'s areas` } }) } };
    }
    return out;
  }

  private send(req: IncomingMessage, res: ServerResponse, out: I3xResponse, headers: Record<string, string> = {}) {
    const p = (req as Scoped).nexusPrincipal;
    if (p?.grant) out = this.filterForGrant(p, out);
    const json = Buffer.from(JSON.stringify(out.body));
    const gz = String(req.headers["accept-encoding"] ?? "").includes("gzip");
    res.writeHead(out.status, { "Content-Type": "application/json", ...(gz ? { "Content-Encoding": "gzip" } : {}), ...headers });
    res.end(gz ? gzipSync(json) : json);
  }
}

function makeDriver(s: SourceDef): Driver {
  switch (s.driver) {
    case "simulator": return new SimulatorDriver(s);
    case "mqtt": return new MqttDriver(s);
    case "opcua": return new OpcUaDriver(s);
    case "sparkplug": return new SparkplugDriver(s);
    case "mediamtx": return new MediaMtxDriver(s, () => gatewayRef);
  }
}

/** The driver factory is module-level; the gateway it polls is the running nexus's. */
let gatewayRef: MediaMtxGateway | null = null;

type Scoped = IncomingMessage & { nexusPrincipal?: Principal };

function namedElements(body: Record<string, unknown> | undefined): string[] {
  if (!body) return [];
  if (Array.isArray(body.elementIds)) return body.elementIds.filter((x): x is string => typeof x === "string");
  if (Array.isArray(body.updates)) return body.updates.map((u) => String((u as { elementId?: unknown })?.elementId ?? ""));
  return [];
}

async function readBody(req: IncomingMessage, max: number): Promise<Buffer | null> {
  const chunks: Buffer[] = [];
  let n = 0;
  for await (const c of req) {
    n += (c as Buffer).length;
    if (n > max) return null;
    chunks.push(c as Buffer);
  }
  return Buffer.concat(chunks);
}

export { shaOfMediaId };
