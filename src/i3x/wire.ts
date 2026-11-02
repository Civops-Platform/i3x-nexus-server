// The i3X 1.0 wire layer: envelopes, the model projection, bulk reads, history,
// subscriptions and the bulk write shape.
//
// Lifted from an earlier i3X server of ours (Full 1.0 Compliance in production),
// which is the reason it is lifted rather than rewritten: every rule below was
// found by CESMII's suite failing an earlier version. What changed for the nexus:
//
//   - the projection is memoised on the source's structure version, because a
//     site nexus serves tens of thousands of objects and rebuilding the model on
//     every request would make each read O(objects);
//   - /info carries `x-nexus` (writable namespaces, priority classes, media
//     transport), and is supplied by the caller;
//   - relationships come from the source, so a type can declare its own
//     (CapturedBy, DerivedFrom, Documents) beside the hierarchy.
//
// The rule that still matters most: i3X 1.0 has no server-side aggregation. A
// request carrying an aggregation parameter is REFUSED, never silently ignored.
//
// Pure and structural: the source and the subscription store are injected, so
// this runs with no database and no network.

import { randomBytes } from "node:crypto";

const UNAUTHENTICATED_PATHS = ["/info"];
export const requiresAuth = (path: string) => !UNAUTHENTICATED_PATHS.includes((path.split("?")[0] || "").replace(/\/+$/, "") || "/");

export interface I3xNamespace {
  uri: string;
  name: string;
  version: string;
}

export interface I3xTypeDef {
  id: string;
  name: string;
  namespaceUri: string;
  attributes: Array<{ name: string; dataType: string; unit?: string; enum?: string[] }>;
}

export interface I3xObject {
  id: string;
  name: string;
  typeId: string;
  parentId?: string | null;
}

/** Value-Quality-Timestamp, per attribute — the fabric seam, not the wire. */
export interface VQT {
  attributeId: string;
  value: number | string | boolean | null;
  quality: "Good" | "GoodNoData" | "Bad" | "Uncertain";
  timestamp: string;
}

/** Everything the server needs, injected. No I/O in this module. */
export interface FabricSource {
  namespaces(): I3xNamespace[];
  types(): I3xTypeDef[];
  objects(): I3xObject[];
  currentValues(attributeIds: readonly string[]): VQT[];
  history(attributeIds: readonly string[], from: string, to: string): VQT[];
  /** Bumped whenever namespaces, types or objects change; the projection is cached on it. */
  structureVersion?(): number;
  /** Non-hierarchical relationships, by source object id: { CapturedBy: [ids] }. */
  relationships?(objectId: string): Record<string, string[]>;
  /** Registered relationship types beyond the hierarchy. */
  relationshipTypes?(): Array<{ elementId: string; reverseOf: string }>;
  /** Server info for GET /info. */
  info?(): Record<string, unknown>;
}

export interface I3xRequest {
  method: "GET" | "POST" | "PUT";
  path: string;
  /** Whether the caller presented a credential the transport accepted. */
  authenticated: boolean;
  query?: Record<string, string | undefined>;
  body?: Record<string, unknown>;
}

export interface I3xResponse {
  status: number;
  body: unknown;
}

// ─── Envelopes ─────────────────────────────────────────────────────────────

export interface ErrorDetail { title: string; status: number; detail: string }

export const ok = (result: unknown, status = 200): I3xResponse => ({ status, body: { success: true, result } });

export const problem = (status: number, title: string, detail: string): I3xResponse => ({
  status,
  body: { success: false, responseDetail: { title, status, detail } },
});

type BulkItem = { success: boolean; result?: unknown; responseDetail?: ErrorDetail } & Record<string, unknown>;

export const bulk = (results: BulkItem[], status = 200): I3xResponse => ({
  status,
  body: { success: results.every((r) => r.success), results },
});

const itemOk = (key: string, id: string, result: unknown): BulkItem => ({ success: true, [key]: id, result });
const itemFail = (key: string, id: string, status: number, title: string, detail: string): BulkItem => ({
  success: false, [key]: id, responseDetail: { title, status, detail },
});
const notFound = (id: string, what = "Element") => itemFail("elementId", id, 404, "Not Found", `${what} not found: ${id}`);

/** Aggregation params a client might send out of habit from other protocols. */
const AGGREGATION_KEYS = ["aggregate", "aggregation", "interval", "resolution", "downsample", "bucket"];

export const RELATIONSHIP_NAMESPACE: I3xNamespace = {
  uri: "urn:i3x-nexus:relationships:1.0",
  name: "Nexus Relationships",
  version: "1.0",
};

/** Hierarchy is the only relationship the fabric carries; both directions are registered. */
export const RELATIONSHIP_TYPES = [
  ["HasParent", "HasChildren"],
  ["HasChildren", "HasParent"],
].map(([id, rev]) => ({
  elementId: id, displayName: id, namespaceUri: RELATIONSHIP_NAMESPACE.uri, relationshipId: id, reverseOf: rev,
}));

export const DEFAULT_INFO = {
  specVersion: "1.0",
  serverVersion: "0.1.0",
  serverName: "i3X Nexus",
  capabilities: {
    query: { history: true },
    update: { current: true, history: true },
    subscribe: { stream: true },
  },
};

const relationshipTypesOf = (fabric: FabricSource) => {
  const extra = fabric.relationshipTypes?.() ?? [];
  return [
    ...RELATIONSHIP_TYPES,
    ...extra.map((r) => ({ elementId: r.elementId, displayName: r.elementId, namespaceUri: RELATIONSHIP_NAMESPACE.uri, relationshipId: r.elementId, reverseOf: r.reverseOf })),
  ];
};

// ─── Subscriptions ─────────────────────────────────────────────────────────

export interface SyncUpdate { elementId: string; value: unknown; quality: VQT["quality"]; timestamp: string }
export interface SyncBatch { sequenceNumber: number; updates: SyncUpdate[] }

export interface Subscription {
  subscriptionId: string;
  clientId: string;
  displayName: string | null;
  monitored: Map<string, { maxDepth: number }>;
  batches: SyncBatch[];
  nextSeq: number;
  /** Last value+quality delivered per element, so a batch carries changes only. */
  lastSeen: Map<string, string>;
  /** Set when the queue cap dropped batches; reported once as a 206. */
  overflowed: boolean;
  /** Bumped by every stream open; a stream whose generation is stale closes itself. */
  streamGeneration: number;
  touchedAt: number;
}

/**
 * In-process subscription state. One server process, one store — which is what
 * a single `next start` serves. Bounded on every axis, because a client that
 * subscribes and never syncs must not be able to grow this without limit.
 */
export interface SubscriptionStore {
  subs: Map<string, Subscription>;
}

export const SUBSCRIPTION_LIMITS = {
  maxSubscriptions: 200,
  maxMonitoredPerSubscription: 5_000,
  maxQueuedBatches: 1_000,
  idleExpiryMs: 60 * 60 * 1000,
};

export const createSubscriptionStore = (): SubscriptionStore => ({ subs: new Map() });

/**
 * Subscriptions outlive a request, and on a serverless host a request is all a
 * process gets — so the route persists the store between calls. These two are
 * the whole wire format of that persistence; the Maps exist only in memory.
 */
export function serializeSubscriptionStore(store: SubscriptionStore): string {
  return JSON.stringify([...store.subs.values()].map((s) => ({
    ...s, monitored: [...s.monitored.entries()], lastSeen: [...s.lastSeen.entries()],
  })));
}

export function deserializeSubscriptionStore(raw: string | null): SubscriptionStore {
  const store = createSubscriptionStore();
  if (!raw) return store;
  let rows: unknown;
  try { rows = JSON.parse(raw); } catch { return store; }
  if (!Array.isArray(rows)) return store;
  for (const r of rows as Array<Record<string, unknown>>) {
    if (typeof r?.subscriptionId !== "string" || typeof r.clientId !== "string") continue;
    store.subs.set(r.subscriptionId, {
      subscriptionId: r.subscriptionId,
      clientId: r.clientId,
      displayName: typeof r.displayName === "string" ? r.displayName : null,
      monitored: new Map(Array.isArray(r.monitored) ? (r.monitored as Array<[string, { maxDepth: number }]>) : []),
      batches: Array.isArray(r.batches) ? (r.batches as SyncBatch[]) : [],
      nextSeq: typeof r.nextSeq === "number" ? r.nextSeq : 1,
      lastSeen: new Map(Array.isArray(r.lastSeen) ? (r.lastSeen as Array<[string, string]>) : []),
      overflowed: r.overflowed === true,
      streamGeneration: typeof r.streamGeneration === "number" ? r.streamGeneration : 0,
      touchedAt: typeof r.touchedAt === "number" ? r.touchedAt : Date.now(),
    });
  }
  return store;
}

// ─── The model, projected from the fabric ──────────────────────────────────

type Json = null | boolean | number | string | Json[] | { [k: string]: Json };

const JSON_TYPE: Record<string, string> = {
  string: "string", integer: "integer", number: "number", double: "number", float: "number", boolean: "boolean",
  object: "object", array: "array",
};

/** A type whose only attribute is `value` is a scalar — a tag — and is served as one. */
const isScalar = (t: I3xTypeDef) => t.attributes.length === 1 && t.attributes[0].name === "value";

function schemaOf(t: I3xTypeDef): Record<string, unknown> {
  const prop = (a: I3xTypeDef["attributes"][number]) => {
    const s: Record<string, unknown> = { type: [JSON_TYPE[a.dataType] ?? "string", "null"] };
    if (a.enum) s.enum = [...a.enum, null];
    if (a.unit) s["x-unit"] = a.unit;
    return s;
  };
  if (isScalar(t)) return prop(t.attributes[0]);
  return { type: "object", properties: Object.fromEntries(t.attributes.map((a) => [a.name, prop(a)])) };
}

/** RFC 3339 UTC, or `fallback` when the source's timestamp is not a time at all. */
function utc(ts: string, fallback: string): string {
  const ms = Date.parse(ts);
  return Number.isNaN(ms) ? fallback : new Date(ms).toISOString();
}

interface Model {
  fabric: FabricSource;
  namespaces: I3xNamespace[];
  types: Map<string, I3xTypeDef>;
  objects: I3xObject[];
  byId: Map<string, I3xObject>;
  children: Map<string, string[]>;
  now: string;
}

const MODEL_CACHE = new WeakMap<FabricSource, { version: number; m: Model }>();

function model(fabric: FabricSource): Model {
  const v = fabric.structureVersion?.();
  if (v !== undefined) {
    const hit = MODEL_CACHE.get(fabric);
    if (hit && hit.version === v) return { ...hit.m, now: new Date().toISOString() };
    const m = buildModel(fabric);
    MODEL_CACHE.set(fabric, { version: v, m });
    return m;
  }
  return buildModel(fabric);
}

function buildModel(fabric: FabricSource): Model {
  const types = new Map(fabric.types().map((t) => [t.id, t]));
  const raw = fabric.objects();
  const served = new Set(raw.map((o) => o.id));
  // A parent this server does not serve is a hierarchy it cannot walk (#150,
  // tests/i3x-object-parents-resolve). It is served as a root, not as a dangling
  // pointer a client finds broken one request later.
  const objects = raw.map((o) => ({ ...o, parentId: o.parentId && served.has(o.parentId) ? o.parentId : null }));
  const children = new Map<string, string[]>();
  for (const o of objects) if (o.parentId) children.set(o.parentId, [...(children.get(o.parentId) ?? []), o.id]);
  const namespaces = [...fabric.namespaces()];
  if (!namespaces.some((n) => n.uri === RELATIONSHIP_NAMESPACE.uri)) namespaces.push(RELATIONSHIP_NAMESPACE);
  return {
    fabric, namespaces, types, objects, children,
    byId: new Map(objects.map((o) => [o.id, o])),
    now: new Date().toISOString(),
  };
}

function relationshipsOf(m: Model, o: I3xObject): Record<string, string[]> {
  const rel: Record<string, string[]> = {};
  if (o.parentId) rel.HasParent = [o.parentId];
  const kids = m.children.get(o.id);
  if (kids?.length) rel.HasChildren = kids;
  for (const [k, ids] of Object.entries(m.fabric.relationships?.(o.id) ?? {})) {
    const served = ids.filter((id) => m.byId.has(id));
    if (served.length) rel[k] = [...(rel[k] ?? []), ...served];
  }
  return rel;
}

function objectResponse(m: Model, o: I3xObject, includeMetadata: boolean) {
  const base: Record<string, unknown> = {
    elementId: o.id,
    displayName: o.name,
    typeElementId: o.typeId,
    parentId: o.parentId ?? null,
    isComposition: false,
    isExtended: false,
  };
  if (includeMetadata) {
    const t = m.types.get(o.typeId);
    base.metadata = {
      description: null,
      typeNamespaceUri: t?.namespaceUri ?? "urn:i3x:unknown",
      sourceTypeId: t?.id ?? "UnknownType",
      relationships: relationshipsOf(m, o),
    };
  }
  return base;
}

const typeResponse = (m: Model, t: I3xTypeDef) => ({
  elementId: t.id,
  displayName: t.name,
  namespaceUri: t.namespaceUri,
  sourceTypeId: t.id,
  version: m.namespaces.find((n) => n.uri === t.namespaceUri)?.version ?? null,
  schema: schemaOf(t),
});

/**
 * Fold attribute VQTs into one object VQT.
 *
 * Quality is the honest worst case: every attribute Bad is Bad; no attribute
 * carrying a value is GoodNoData with a null value; any attribute Bad or
 * Uncertain among real values makes the whole Uncertain. A null value is never
 * served as Good — QRY-02, and the reason `no-data-not-zero` exists.
 */
function fold(t: I3xTypeDef, vqts: VQT[], now: string): { value: Json; quality: VQT["quality"]; timestamp: string } {
  if (vqts.length === 0) return { value: null, quality: "GoodNoData", timestamp: now };
  const timestamp = vqts.map((v) => utc(v.timestamp, now)).sort().at(-1)!;
  if (isScalar(t)) {
    const v = vqts[0];
    if (v.value === null || v.value === undefined) return { value: null, quality: v.quality === "Bad" ? "Bad" : "GoodNoData", timestamp };
    return { value: v.value, quality: v.quality === "Bad" || v.quality === "GoodNoData" ? "Uncertain" : v.quality, timestamp };
  }
  const present = vqts.filter((v) => v.value !== null && v.value !== undefined);
  if (present.length === 0) {
    return { value: null, quality: vqts.every((v) => v.quality === "Bad") ? "Bad" : "GoodNoData", timestamp };
  }
  const value: Record<string, Json> = {};
  t.attributes.forEach((a, i) => { value[a.name] = (vqts[i]?.value ?? null) as Json; });
  const degraded = vqts.some((v) => v.quality === "Bad" || v.quality === "Uncertain");
  return { value, quality: degraded ? "Uncertain" : "Good", timestamp };
}

const attrIds = (o: I3xObject, t: I3xTypeDef) => t.attributes.map((a) => `${o.id}/${a.name}`);

function currentValue(m: Model, o: I3xObject) {
  const t = m.types.get(o.typeId);
  if (!t) return { value: null, quality: "Bad" as const, timestamp: m.now };
  return fold(t, m.fabric.currentValues(attrIds(o, t)), m.now);
}

function historyOf(m: Model, o: I3xObject, from: string, to: string, lo: number, hi: number) {
  const t = m.types.get(o.typeId);
  if (!t) return [];
  const ids = attrIds(o, t);
  // Points only. A null placeholder is how a fabric says "nothing in range";
  // on the wire that is an empty array, not a fabricated sample at now().
  const pts = m.fabric.history(ids, from, to).filter((v) => {
    if (v.value === null || v.value === undefined) return false;
    const ms = Date.parse(v.timestamp);
    return !Number.isNaN(ms) && ms >= lo && ms <= hi;
  });
  const byTs = new Map<string, VQT[]>();
  for (const p of pts) {
    const ts = utc(p.timestamp, m.now);
    byTs.set(ts, [...(byTs.get(ts) ?? []), p]);
  }
  return [...byTs.keys()].sort().map((ts) => {
    const at = byTs.get(ts)!;
    // Re-align to the attribute order so a missing attribute at this instant is null, not shifted.
    const aligned = ids.map((id) => at.find((p) => p.attributeId === id) ?? { attributeId: id, value: null, quality: "GoodNoData" as const, timestamp: ts });
    return fold(t, aligned, m.now);
  });
}

// ─── Dispatch ──────────────────────────────────────────────────────────────

function idsFrom(body: Record<string, unknown> | undefined, key = "elementIds"): string[] | null {
  const raw = body?.[key];
  if (!Array.isArray(raw)) return null;
  return raw.filter((x): x is string => typeof x === "string");
}

const RFC3339 = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:\d{2})$/i;

export function handleI3xRequest(
  req: I3xRequest,
  fabric: FabricSource,
  store: SubscriptionStore = createSubscriptionStore(),
): I3xResponse {
  const path = (req.path.split("?")[0] || "/").replace(/\/+$/, "") || "/";

  // #156: everything except GET /info requires a credential.
  if (requiresAuth(path) && !req.authenticated) {
    return problem(401, "Unauthorized", "i3X requires authentication for every endpoint except GET /info.");
  }

  // No server-side aggregation, on ANY endpoint — refused, never ignored.
  const asked = AGGREGATION_KEYS.filter((k) => req.query?.[k] !== undefined || req.body?.[k] !== undefined);
  if (asked.length) {
    return problem(
      400, "Aggregation not supported",
      `i3X 1.0 defines no server-side aggregation; this request asked for ${asked.join(", ")}. ` +
      "Pull raw VQT from /objects/history and roll up client-side. Returning a raw series to a caller who " +
      "believes it was aggregated would be a wrong number they cannot see.",
    );
  }

  const route = `${req.method} ${path}`;
  if (route === "GET /info") return ok(fabric.info?.() ?? DEFAULT_INFO);

  if (path.startsWith("/subscriptions")) return subscriptions(req, path, fabric, store);

  // Writes are the transport's (they need a store, not a projection); it answers
  // them through `handleWrites`. Reaching here means it was given no store.
  if (route === "PUT /objects/value" || route === "PUT /objects/history") {
    return problem(503, "Writes unavailable", "This transport has no store to write to.");
  }

  const m = model(fabric);
  const q = req.query ?? {};

  switch (route) {
    case "GET /namespaces":
      return ok(m.namespaces.map((n) => ({ uri: n.uri, displayName: n.name, version: n.version })));

    case "GET /objecttypes": {
      const ns = q.namespaceUri;
      return ok([...m.types.values()].filter((t) => !ns || t.namespaceUri === ns).map((t) => typeResponse(m, t)));
    }
    case "POST /objecttypes/query": {
      const ids = idsFrom(req.body);
      if (!ids) return problem(400, "Bad Request", "elementIds array is required");
      return bulk(ids.map((id) => {
        const t = m.types.get(id);
        return t ? itemOk("elementId", id, typeResponse(m, t)) : notFound(id, "Object type");
      }));
    }

    case "GET /relationshiptypes": {
      const ns = q.namespaceUri;
      return ok(relationshipTypesOf(fabric).filter((r) => !ns || r.namespaceUri === ns));
    }
    case "POST /relationshiptypes/query": {
      const ids = idsFrom(req.body);
      if (!ids) return problem(400, "Bad Request", "elementIds array is required");
      return bulk(ids.map((id) => {
        const r = relationshipTypesOf(fabric).find((x) => x.elementId === id);
        return r ? itemOk("elementId", id, r) : notFound(id, "Relationship type");
      }));
    }

    case "GET /objects": {
      let list = m.objects;
      if (q.typeElementId) list = list.filter((o) => o.typeId === q.typeElementId);
      if (q.root === "true") list = list.filter((o) => !o.parentId);
      return ok(list.map((o) => objectResponse(m, o, q.includeMetadata === "true")));
    }
    case "POST /objects/list": {
      const ids = idsFrom(req.body);
      if (!ids) return problem(400, "Bad Request", "elementIds array is required");
      const meta = req.body?.includeMetadata === true;
      return bulk(ids.map((id) => {
        const o = m.byId.get(id);
        return o ? itemOk("elementId", id, objectResponse(m, o, meta)) : notFound(id);
      }));
    }
    case "POST /objects/related": {
      const ids = idsFrom(req.body);
      if (!ids) return problem(400, "Bad Request", "elementIds array is required");
      const only = typeof req.body?.relationshipType === "string" ? req.body.relationshipType : null;
      const meta = req.body?.includeMetadata === true;
      return bulk(ids.map((id) => {
        const o = m.byId.get(id);
        if (!o) return notFound(id);
        const edges: Array<{ sourceRelationship: string; object: unknown }> = [];
        for (const [rel, targets] of Object.entries(relationshipsOf(m, o))) {
          if (only && rel !== only) continue;
          for (const tid of targets) {
            const t = m.byId.get(tid);
            if (t) edges.push({ sourceRelationship: rel, object: objectResponse(m, t, meta) });
          }
        }
        return itemOk("elementId", id, edges);
      }));
    }

    case "POST /objects/value": {
      const ids = idsFrom(req.body);
      if (!ids) return problem(400, "Bad Request", "elementIds array is required");
      return bulk(ids.map((id) => {
        const o = m.byId.get(id);
        return o ? itemOk("elementId", id, { isComposition: false, ...currentValue(m, o) }) : notFound(id);
      }));
    }

    case "POST /objects/history": {
      const ids = idsFrom(req.body);
      if (!ids) return problem(400, "Bad Request", "elementIds array is required");
      const from = req.body?.startTime;
      const to = req.body?.endTime;
      if (typeof from !== "string" || typeof to !== "string") {
        return problem(400, "Bad Request", "startTime and endTime are required (RFC 3339)");
      }
      const lo = Date.parse(from);
      const hi = Date.parse(to);
      if (!RFC3339.test(from) || !RFC3339.test(to) || Number.isNaN(lo) || Number.isNaN(hi)) {
        return problem(400, "Bad Request", "startTime and endTime must be valid RFC 3339 timestamps");
      }
      if (lo > hi) {
        return problem(400, "Bad Request", `startTime (${from}) is after endTime (${to}); an inverted range would return an empty series that reads as 'no data'`);
      }
      return bulk(ids.map((id) => {
        const o = m.byId.get(id);
        return o ? itemOk("elementId", id, { isComposition: false, values: historyOf(m, o, from, to, lo, hi) }) : notFound(id);
      }));
    }
  }

  return problem(404, "Not Found", `${req.method} ${path} is not an i3X 1.0 endpoint`);
}

// ─── Subscription endpoints ────────────────────────────────────────────────

function expire(store: SubscriptionStore, now: number) {
  for (const [id, s] of store.subs) if (now - s.touchedAt > SUBSCRIPTION_LIMITS.idleExpiryMs) store.subs.delete(id);
}

/**
 * Capture the changes since the last delivery as one batch. Change means value
 * or quality — not timestamp, which for nameplate data is read time and would
 * make every sync a batch of nothing new.
 */
function changes(m: Model, ids: Iterable<string>, lastSeen: Map<string, string>): SyncUpdate[] {
  const updates: SyncUpdate[] = [];
  for (const id of ids) {
    const o = m.byId.get(id);
    if (!o) continue;
    const v = currentValue(m, o);
    const key = JSON.stringify([v.value, v.quality]);
    if (lastSeen.get(id) === key) continue;
    lastSeen.set(id, key);
    updates.push({ elementId: id, value: v.value, quality: v.quality, timestamp: v.timestamp });
  }
  return updates;
}

/** The changes since `lastSeen` for these objects — what one stream tick sends. Updates `lastSeen`. */
export const changesSince = (fabric: FabricSource, ids: Iterable<string>, lastSeen: Map<string, string>) =>
  changes(model(fabric), ids, lastSeen);

function capture(m: Model, s: Subscription) {
  const updates = changes(m, s.monitored.keys(), s.lastSeen);
  if (!updates.length) return;
  s.batches.push({ sequenceNumber: s.nextSeq++, updates });
  if (s.batches.length > SUBSCRIPTION_LIMITS.maxQueuedBatches) {
    s.batches.splice(0, s.batches.length - SUBSCRIPTION_LIMITS.maxQueuedBatches);
    s.overflowed = true;
  }
}

function subscriptions(req: I3xRequest, path: string, fabric: FabricSource, store: SubscriptionStore): I3xResponse {
  if (req.method !== "POST") return problem(404, "Not Found", `${req.method} ${path} is not an i3X 1.0 endpoint`);
  const body = req.body ?? {};
  const clientId = typeof body.clientId === "string" && body.clientId.trim() ? body.clientId : null;
  // "The client MUST pass in a clientId ... to scope the subscription to the client."
  if (!clientId) return problem(400, "Bad Request", "clientId is required");

  const nowMs = Date.now();
  expire(store, nowMs);
  // Another client's subscription is indistinguishable from a nonexistent one.
  const owned = (id: unknown): Subscription | null => {
    const s = typeof id === "string" ? store.subs.get(id) : undefined;
    if (!s || s.clientId !== clientId) return null;
    s.touchedAt = nowMs;
    return s;
  };
  const subNotFound = (id: string) => itemFail("subscriptionId", id, 404, "Not Found", `Subscription not found: ${id}`);

  switch (path) {
    case "/subscriptions": {
      if (store.subs.size >= SUBSCRIPTION_LIMITS.maxSubscriptions) {
        return problem(429, "Too Many Subscriptions", `This server holds at most ${SUBSCRIPTION_LIMITS.maxSubscriptions} subscriptions; delete unused ones first.`);
      }
      const subscriptionId = randomBytes(24).toString("base64url");
      const displayName = typeof body.displayName === "string" ? body.displayName : null;
      store.subs.set(subscriptionId, {
        subscriptionId, clientId, displayName, monitored: new Map(), batches: [], nextSeq: 1,
        lastSeen: new Map(), overflowed: false, streamGeneration: 0, touchedAt: nowMs,
      });
      return ok({ clientId, subscriptionId, displayName });
    }

    case "/subscriptions/list": {
      const ids = idsFrom(body, "subscriptionIds");
      if (!ids) return problem(400, "Bad Request", "subscriptionIds array is required");
      return bulk(ids.map((id) => {
        const s = owned(id);
        if (!s) return subNotFound(id);
        return itemOk("subscriptionId", id, {
          subscriptionId: id,
          displayName: s.displayName,
          monitoredObjects: [...s.monitored.entries()].map(([elementId, c]) => ({ elementId, maxDepth: c.maxDepth })),
        });
      }));
    }

    case "/subscriptions/delete": {
      const ids = idsFrom(body, "subscriptionIds");
      if (!ids) return problem(400, "Bad Request", "subscriptionIds array is required");
      return bulk(ids.map((id) => {
        if (!owned(id)) return subNotFound(id);
        store.subs.delete(id);
        return itemOk("subscriptionId", id, null);
      }));
    }

    case "/subscriptions/register":
    case "/subscriptions/unregister": {
      const ids = idsFrom(body);
      if (typeof body.subscriptionId !== "string" || !ids) {
        return problem(400, "Bad Request", "subscriptionId and elementIds are required");
      }
      const s = owned(body.subscriptionId);
      if (!s) return problem(404, "Not Found", `Subscription not found: ${body.subscriptionId}`);
      if (path === "/subscriptions/unregister") {
        return bulk(ids.map((id) => {
          if (!s.monitored.has(id)) return itemFail("elementId", id, 404, "Not Found", `Element not registered: ${id}`);
          s.monitored.delete(id);
          s.lastSeen.delete(id);
          return itemOk("elementId", id, null);
        }));
      }
      const m = model(fabric);
      const maxDepth = typeof body.maxDepth === "number" ? body.maxDepth : 1;
      return bulk(ids.map((id) => {
        if (!m.byId.has(id)) return notFound(id);
        if (s.monitored.has(id)) return itemOk("elementId", id, null); // idempotent: later registrations are ignored
        if (s.monitored.size >= SUBSCRIPTION_LIMITS.maxMonitoredPerSubscription) {
          return itemFail("elementId", id, 429, "Too Many Monitored Objects", `A subscription monitors at most ${SUBSCRIPTION_LIMITS.maxMonitoredPerSubscription} objects.`);
        }
        s.monitored.set(id, { maxDepth });
        return itemOk("elementId", id, null);
      }));
    }

    case "/subscriptions/sync": {
      if (typeof body.subscriptionId !== "string") return problem(400, "Bad Request", "subscriptionId is required");
      const s = owned(body.subscriptionId);
      if (!s) return problem(404, "Not Found", `Subscription not found: ${body.subscriptionId}`);
      const last = body.lastSequenceNumber;
      if (last === -1) s.batches = [];
      else if (typeof last === "number" && last >= 0) s.batches = s.batches.filter((b) => b.sequenceNumber > last);
      capture(model(fabric), s);
      if (s.overflowed) {
        s.overflowed = false;
        return {
          status: 206,
          body: {
            success: true,
            result: s.batches,
            responseDetail: {
              title: "Partial Content", status: 206,
              detail: `The queue exceeded ${SUBSCRIPTION_LIMITS.maxQueuedBatches} unacknowledged batches and the oldest were dropped. Sync more often or acknowledge with lastSequenceNumber.`,
            },
          },
        };
      }
      return ok(s.batches);
    }

    case "/subscriptions/stream": {
      // Opening the stream is bookkeeping, done here so every transport agrees on
      // it; holding it open is the transport's (`i3xStream.ts`). The generation is
      // how a second stream closes the first even when another process holds it.
      const s = owned(body.subscriptionId);
      if (!s) return problem(404, "Not Found", `Subscription not found: ${String(body.subscriptionId)}`);
      s.streamGeneration += 1;
      capture(model(fabric), s);
      const pending = s.batches.flatMap((b) => b.updates);
      s.batches = []; // delivered on the stream that is about to open
      return ok({ subscriptionId: s.subscriptionId, generation: s.streamGeneration, pending, lastSeen: [...s.lastSeen.entries()] });
    }
  }

  return problem(404, "Not Found", `${req.method} ${path} is not an i3X 1.0 endpoint`);
}

// ─── Writes ────────────────────────────────────────────────────────────────

/** `id` is what was written, when the writer can name it — a success that carries proof (#847). */
export type WriteOutcome = { ok: true; id?: string } | { ok: false; status: number; detail: string };

const TITLES: Record<number, string> = { 400: "Bad Request", 403: "Forbidden", 404: "Not Found", 500: "Write failed" };

/**
 * The bulk shape of `PUT /objects/value` and `PUT /objects/history`, shared by
 * every transport: an `updates` array answered item by item, in request order.
 * An unknown elementId is a per-item 404 and never sinks the batch; what an
 * item means — written, read-only, malformed — is the transport's `write`.
 */
export async function handleWrites(
  body: Record<string, unknown> | undefined,
  known: (elementId: string) => boolean,
  write: (update: unknown, elementId: string, index: number) => Promise<WriteOutcome> | WriteOutcome,
): Promise<I3xResponse> {
  const updates = body?.updates;
  if (!Array.isArray(updates)) return problem(400, "Bad Request", "updates array is required");
  const results: BulkItem[] = [];
  for (const [i, u] of updates.entries()) {
    const id = String((u as { elementId?: unknown })?.elementId ?? "");
    if (!known(id)) { results.push(notFound(id)); continue; }
    const out = await write(u, id, i);
    results.push(out.ok ? itemOk("elementId", id, null) : itemFail("elementId", id, out.status, TITLES[out.status] ?? "Write failed", out.detail));
  }
  return bulk(results);
}
