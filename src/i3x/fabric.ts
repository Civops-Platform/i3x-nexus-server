// The nexus as an i3X FabricSource: one address space over the site model, the
// last-value cache, owned records and media (spec §5).
//
// The wire layer (`wire.ts`) speaks objects; everything here speaks attributes
// (`<objectId>/<attribute>`), which is how each store is addressed and tested.
// This module is where the two meet, and where each data class answers the same
// three questions differently: what is the current value, what is its history,
// and who may write it.

import type { FabricSource, I3xNamespace, I3xObject, I3xTypeDef, VQT } from "./wire";
import type { ObjectDef, PriorityClass, SiteConfig, TypeDef } from "../model/config";
import type { LastValueCache } from "../core/cache";
import type { RecordStore } from "../core/records";
import type { MediaItem, MediaStore } from "../core/media";
import type { Lanes } from "../core/priority";

export const NEXUS_NS: I3xNamespace = { uri: "urn:i3x-nexus:1.0", name: "i3X Nexus", version: "1.0" };

/** Built-in types every nexus serves, whatever the site declares. */
export const BUILTIN_TYPES: TypeDef[] = [
  {
    id: "MediaItem", name: "Media item", namespaceUri: NEXUS_NS.uri, kind: "media", priority: "P3",
    attributes: [
      { name: "sha256", dataType: "string" }, { name: "mimeType", dataType: "string" }, { name: "size", dataType: "integer" },
      { name: "status", dataType: "string", enum: ["awaiting_bytes", "available"] },
      { name: "capturedAt", dataType: "string" }, { name: "capturedBy", dataType: "string" }, { name: "documents", dataType: "string" },
      { name: "durationMs", dataType: "number" }, { name: "width", dataType: "integer" }, { name: "height", dataType: "integer" },
      { name: "description", dataType: "string" }, { name: "href", dataType: "string" },
    ],
  },
  {
    id: "MediaIntake", name: "Media upload intake", namespaceUri: NEXUS_NS.uri, kind: "record", priority: "P2", write: "W0",
    attributes: [{ name: "accepts", dataType: "string" }, { name: "lastRequested", dataType: "string" }, { name: "maxBytes", dataType: "integer" }],
  },
  { id: "NexusFolder", name: "Folder", namespaceUri: NEXUS_NS.uri, kind: "folder", attributes: [] },
  {
    id: "WriteRequestIntake", name: "Write request intake (W1)", namespaceUri: NEXUS_NS.uri, kind: "record", priority: "P2", write: "W0",
    attributes: [{ name: "accepts", dataType: "string" }, { name: "lastRequested", dataType: "string" }],
  },
  {
    // Annex A. Status and escalation are computed by the governor (NX-23).
    id: "WriteRequest", name: "Write request", namespaceUri: NEXUS_NS.uri, kind: "record", priority: "P0",
    attributes: [
      { name: "target", dataType: "object" }, { name: "tier", dataType: "string" }, { name: "proposed", dataType: "object" },
      { name: "prior", dataType: "object" }, { name: "actor", dataType: "object" }, { name: "justification", dataType: "object" },
      { name: "matter", dataType: "object" }, { name: "authority", dataType: "object" }, { name: "reversible", dataType: "boolean" },
      { name: "compensation", dataType: "object" }, { name: "compensates", dataType: "string" }, { name: "idempotencyKey", dataType: "string" },
      { name: "status", dataType: "string", enum: ["requested", "approved", "applied", "verified", "rejected", "expired", "rolled_back"] },
      { name: "statusDetail", dataType: "string" }, { name: "approvals", dataType: "array" }, { name: "applied", dataType: "object" },
      { name: "verification", dataType: "object" }, { name: "expiresAt", dataType: "string" },
    ],
  },
];

export const MEDIA_FOLDER = "nexus:media";
export const MEDIA_INTAKE = "nexus:intake:media";
export const W1_INTAKE = "nexus:intake:W1";
export const W2_INTAKE = "nexus:intake:W2";
export const REQUESTS_FOLDER = "nexus:requests";
export const mediaId = (sha: string) => `nexus:media:${sha}`;
export const shaOfMediaId = (id: string) => (/^nexus:media:([0-9a-f]{64})$/.exec(id) ?? [])[1] ?? null;

const BUILTIN_OBJECTS: ObjectDef[] = [
  { id: REQUESTS_FOLDER, name: "Write requests", typeId: "NexusFolder", parentId: null },
  { id: W1_INTAKE, name: "Write request intake (W1)", typeId: "WriteRequestIntake", parentId: REQUESTS_FOLDER },
  { id: W2_INTAKE, name: "Setpoint request intake (W2)", typeId: "WriteRequestIntake", parentId: REQUESTS_FOLDER },
  { id: MEDIA_FOLDER, name: "Media", typeId: "NexusFolder", parentId: null },
  { id: MEDIA_INTAKE, name: "Media upload intake", typeId: "MediaIntake", parentId: MEDIA_FOLDER },
];

const DEFAULT_PRIORITY: Record<string, PriorityClass> = { realtime: "P1", record: "P2", media: "P3", stream: "P1", folder: "P1", external: "P2", mounted: "P1" };

export interface NexusDeps {
  config: SiteConfig;
  cache: LastValueCache;
  records: RecordStore;
  media: MediaStore;
  lanes?: Lanes;
  /** Absolute base for signed media URLs, e.g. https://nexus.example/v1 */
  publicBase: () => string;
  /** Redundancy state for /info (profile Annex B). */
  redundancy?: () => Record<string, unknown>;
  /** Highest profile level this deployment claims. */
  level?: () => number;
  /** Projected system-of-record objects (W1 targets) and their current fields. */
  external?: { objects: () => ObjectDef[]; value: (elementId: string) => { fields: Record<string, unknown>; version: string } | null };
  /** Signed viewer URL for a gateway-served stream, or null. */
  viewerUrl?: (o: ObjectDef) => string | null;
  /** Which tiers are on, for /info. */
  tiers?: () => Record<string, string>;
}

export class NexusFabric implements FabricSource {
  private version = 1;
  private typeMap = new Map<string, TypeDef>();
  private defs = new Map<string, ObjectDef>();
  /** Objects mirrored from upstream site nexuses: upstream id → its defs. */
  private mounts = new Map<string, { types: TypeDef[]; objects: ObjectDef[] }>();
  private mountOf_ = new Map<string, string>();
  private order: ObjectDef[] = [];
  private rel = new Map<string, Record<string, string[]>>();
  private reverse = new Map<string, string>();
  /** Set by the transport for the span of one history request (see server.ts). */
  prefetched: VQT[] | null = null;

  constructor(private d: NexusDeps) {
    for (const t of [...BUILTIN_TYPES, ...d.config.types]) this.typeMap.set(t.id, t);
    for (const [a, b] of d.config.relationshipTypes ?? []) { this.reverse.set(a, b); this.reverse.set(b, a); }
    this.reverse.set("CapturedBy", "Captured");
    this.reverse.set("Captured", "CapturedBy");
    this.reverse.set("Documents", "DocumentedBy");
    for (const [a, b] of [["Targets", "TargetedBy"], ["GovernedBy", "Governs"], ["JustifiedBy", "Justifies"], ["Compensates", "CompensatedBy"]]) {
      this.reverse.set(a, b); this.reverse.set(b, a);
    }
    this.reverse.set("DocumentedBy", "Documents");
    this.rebuild();
  }

  /** Recompute the object list and relationship index; bumps the structure version. */
  rebuild() {
    const media: ObjectDef[] = this.d.media.all().map((m) => ({
      id: mediaId(m.sha256), name: m.description ?? `${m.mimeType} ${m.sha256.slice(0, 12)}`, typeId: "MediaItem",
      parentId: MEDIA_FOLDER,
      relationships: {
        ...(m.capturedBy ? { CapturedBy: [m.capturedBy] } : {}),
        ...(m.documents ? { Documents: [m.documents] } : {}),
      },
    }));
    // Owned writable records lead the listing: CESMII's suite samples ONE object
    // per type for values and writes to the first that holds one (see README).
    const mounted = [...this.mounts.values()].flatMap((m) => m.objects);
    const external = this.d.external?.objects() ?? [];
    // WriteRequests are records the governor creates; each is related to what it targets,
    // the limit that governs it, the decision that justifies it, and what it compensates.
    const requests: ObjectDef[] = this.d.records.ids().filter((id) => id.startsWith("nexus:wr:")).map((id) => {
      const v = (this.d.records.current(id)?.value ?? {}) as Record<string, any>;
      const rel: Record<string, string[]> = {};
      const known = (x: unknown) => typeof x === "string" && x.length > 0;
      if (known(v.target?.elementId)) rel.Targets = [v.target.elementId];
      if (known(v.authority?.limitRef)) rel.GovernedBy = [v.authority.limitRef];
      if (known(v.justification?.decisionRef)) rel.JustifiedBy = [v.justification.decisionRef];
      if (known(v.compensates)) rel.Compensates = [v.compensates];
      return { id, name: `${v.status ?? "request"}: ${v.target?.elementId ?? ""}`, typeId: "WriteRequest", parentId: REQUESTS_FOLDER, relationships: rel };
    });
    const all = [...this.d.config.objects, ...BUILTIN_OBJECTS, ...media, ...mounted, ...external, ...requests];
    const lead = (o: ObjectDef) => (this.typeMap.get(o.typeId)?.write === "W0" && o.typeId !== "MediaIntake" && o.typeId !== "WriteRequestIntake" ? 0 : 1);
    this.order = [...all].sort((a, b) => lead(a) - lead(b));
    this.defs = new Map(all.map((o) => [o.id, o]));
    this.rel = new Map();
    const add = (from: string, k: string, to: string) => {
      const r = this.rel.get(from) ?? {};
      if (!(r[k] ??= []).includes(to)) r[k].push(to);
      this.rel.set(from, r);
    };
    for (const o of all) {
      for (const [k, targets] of Object.entries(o.relationships ?? {})) {
        for (const t of targets) {
          add(o.id, k, t);
          const rev = this.reverse.get(k);
          if (rev) add(t, rev, o.id);
        }
      }
    }
    this.version++;
  }

  // ── FabricSource ────────────────────────────────────────────────────────

  structureVersion() { return this.version; }

  /** A cluster swaps in the shared ledger once Postgres is reachable. */
  useRecords(r: RecordStore) { this.d.records = r; this.rebuild(); }

  /**
   * Mount an upstream's address space (NX-48). Element ids are kept, so a client
   * that knows an element at the site finds it here under the same id. Values come
   * from the cache, fed by the mirror's stream; each attribute is bound to the
   * upstream as its source, so losing the uplink degrades them like any lost source.
   */
  mount(upstreamId: string, types: TypeDef[], objects: ObjectDef[]) {
    const prev = this.mounts.get(upstreamId);
    for (const o of prev?.objects ?? []) this.mountOf_.delete(o.id);
    for (const t of types) this.typeMap.set(t.id, t);
    this.mounts.set(upstreamId, { types, objects });
    for (const o of objects) this.mountOf_.set(o.id, upstreamId);
    this.rebuild();
  }

  mountOf(id: string) { return this.mountOf_.get(id) ?? null; }

  namespaces(): I3xNamespace[] {
    const ns = [...this.d.config.namespaces];
    if (!ns.some((n) => n.uri === NEXUS_NS.uri)) ns.push(NEXUS_NS);
    return ns;
  }

    types(): I3xTypeDef[] {
    return [...this.typeMap.values()].map((t) => ({
      id: t.id, name: t.name, namespaceUri: t.namespaceUri,
      attributes: t.attributes.map((a) => ({ name: a.name, dataType: a.dataType, unit: a.unit, enum: a.enum })),
    }));
  }

  objects(): I3xObject[] {
    return this.order.map((o) => ({ id: o.id, name: o.name, typeId: o.typeId, parentId: o.parentId ?? null }));
  }

  relationships(objectId: string) { return this.rel.get(objectId) ?? {}; }

  relationshipTypes() {
    return [...this.reverse.entries()].map(([elementId, reverseOf]) => ({ elementId, reverseOf }));
  }

  currentValues(attributeIds: readonly string[]): VQT[] {
    const out: VQT[] = [];
    const now = Date.now();
    for (const attributeId of attributeIds) {
      const { obj, attr } = split(attributeId);
      const o = this.defs.get(obj);
      const t = o && this.typeMap.get(o.typeId);
      if (!o || !t) continue;
      const v = this.valueOf(o, t, attr, now);
      if (v) out.push({ attributeId, ...v });
    }
    // The wire layer folds by attribute position; keep the requested order and
    // fill gaps with GoodNoData so a missing attribute is null, not shifted.
    if (out.length === 0) return out;
    return attributeIds.map((id) => out.find((v) => v.attributeId === id) ?? { attributeId: id, value: null, quality: "GoodNoData", timestamp: new Date(now).toISOString() });
  }

  history(attributeIds: readonly string[], from: string, to: string): VQT[] {
    const lo = Date.parse(from), hi = Date.parse(to);
    const out: VQT[] = [];
    for (const attributeId of attributeIds) {
      const { obj, attr } = split(attributeId);
      const o = this.defs.get(obj);
      const t = o && this.typeMap.get(o.typeId);
      if (!o || !t) continue;
      if (t.kind === "record") {
        for (const v of this.d.records.history(o.id)) {
          const ms = Date.parse(v.timestamp);
          if (ms < lo || ms > hi) continue;
          out.push({ attributeId, value: fieldOf(v.value, attr), quality: "Good", timestamp: v.timestamp });
        }
      }
    }
    if (this.prefetched) out.push(...this.prefetched.filter((p) => attributeIds.includes(p.attributeId)));
    return out;
  }

  info() {
    const writable = [...this.typeMap.values()].filter((t) => t.write === "W0").map((t) => t.namespaceUri);
    return {
      specVersion: "1.0",
      serverVersion: "0.1.0",
      serverName: `i3X Nexus · ${this.d.config.site.name}`,
      capabilities: { query: { history: true }, update: { current: true, history: true }, subscribe: { stream: true } },
      "x-nexus": {
        profileVersion: "0.4",
        level: this.d.level?.() ?? 1,
        site: this.d.config.site.id,
        writeTiers: this.d.tiers?.() ?? { W0: "enabled", W1: "disabled", W2: "disabled" },
        intakes: { W1: this.d.tiers?.().W1 === "enabled" ? W1_INTAKE : null, W2: this.d.tiers?.().W2 === "enabled" ? W2_INTAKE : null, media: MEDIA_INTAKE },
        writableNamespaces: [...new Set(writable)],
        priorityClasses: {
          P0: "alarms, quality changes, approvals, rollbacks; reserved capacity, never shed",
          P1: "realtime values and subscriptions; conflated under load",
          P2: "records and W0 writes; queued, then 429",
          P3: "history ranges and media bytes; shed first, 503",
        },
        media: {
          intake: MEDIA_INTAKE,
          transport: "bytes out of band: PUT/GET the signed href on each MediaItem; live streams via the stream descriptor's viewerUrl",
          maxBytes: this.d.media.maxBytes,
        },
        redundancy: this.d.redundancy?.() ?? { mode: "single", state: "ok", endpoints: [this.d.publicBase()] },
        history: {
          owned: "event log window, plus PostgreSQL when configured",
          federated: (this.d.config.historians ?? []).map((h) => `${h.id} (${h.kind}${h.dialect && h.dialect !== "postgres" ? `/${h.dialect}` : ""}, provider ${h.provider})`),
        },
        // Additive (Annex B): lets an enterprise mirror replay each type in its class (NX-45).
        priorityByType: Object.fromEntries([...this.typeMap.values()].map((t) => [t.id, t.priority ?? DEFAULT_PRIORITY[t.kind] ?? "P1"])),
        serverSideAggregation: false,
        lanes: this.d.lanes?.stats ?? null,
      },
    };
  }

  // ── Helpers the transport uses ──────────────────────────────────────────

  objectDef(id: string) { return this.defs.get(id); }
  typeOf(id: string) { const o = this.defs.get(id); return o ? this.typeMap.get(o.typeId) : undefined; }
  priorityOf(id: string): PriorityClass {
    const t = this.typeOf(id);
    return t?.priority ?? DEFAULT_PRIORITY[t?.kind ?? "realtime"] ?? "P1";
  }
  realtimeAttributeIds(objectIds: readonly string[]): string[] {
    const out: string[] = [];
    for (const id of objectIds) {
      const t = this.typeOf(id);
      if (t?.kind === "realtime" || t?.kind === "stream" || t?.kind === "mounted") for (const a of t.attributes) out.push(`${id}/${a.name}`);
    }
    return out;
  }

  private valueOf(o: ObjectDef, t: TypeDef, attr: string, now: number): Omit<VQT, "attributeId"> | null {
    const bound = o.bindings?.[attr];
    if (bound) {
      const c = this.d.cache.get(`${o.id}/${attr}`);
      if (!c) return null;
      // Bad is served without a value. The wire layer folds "Bad with a value" into
      // Uncertain, which would show a lost source as merely doubtful (spec NX-2).
      // The last good value stays readable in history.
      return { value: c.quality === "Bad" ? null : c.value, quality: c.quality, timestamp: c.timestamp };
    }
    switch (t.kind) {
      case "record": {
        if (t.id === "WriteRequestIntake") {
          const last = this.d.records.ids().filter((i) => i.startsWith("nexus:wr:")).at(-1) ?? null;
          const v: Record<string, unknown> = { accepts: "WriteRequest {target, proposed, exceptionType, exposureUsd?, boundaryTriggers?, justification?, reversible?, compensation?, idempotencyKey} or {compensates, idempotencyKey}", lastRequested: last };
          return { value: v[attr] as VQT["value"], quality: "Good", timestamp: new Date(0).toISOString() };
        }
        if (t.id === "MediaIntake") {
          const last = this.d.media.all().at(-1);
          const v: Record<string, unknown> = { accepts: "MediaRequest {sha256, mimeType, size, capturedAt?, capturedBy?, documents?, durationMs?, width?, height?, description?}", lastRequested: last ? mediaId(last.sha256) : null, maxBytes: this.d.media.maxBytes };
          return { value: v[attr] as VQT["value"], quality: "Good", timestamp: last?.requestedAt ?? new Date(now).toISOString() };
        }
        const cur = this.d.records.current(o.id);
        return cur ? { value: fieldOf(cur.value, attr), quality: "Good", timestamp: cur.timestamp } : null;
      }
      case "media": {
        const sha = shaOfMediaId(o.id);
        const m = sha ? this.d.media.get(sha) : undefined;
        if (!m) return null;
        if (attr === "href") return { value: this.href(m, now), quality: "Good", timestamp: m.requestedAt };
        const v = (m as unknown as Record<string, unknown>)[attr];
        return { value: (v ?? null) as VQT["value"], quality: "Good", timestamp: m.capturedAt ?? m.requestedAt };
      }
      case "external": {
        const e = this.d.external?.value(o.id);
        if (!e) return null;
        return { value: (e.fields[attr] ?? null) as VQT["value"], quality: "Good", timestamp: new Date(0).toISOString() };
      }
      case "stream": {
        if (attr === "viewerUrl") {
          const signed = this.d.viewerUrl?.(o);
          if (signed) return { value: signed, quality: "Good", timestamp: new Date(0).toISOString() };
        }
        const s = o.stream as Record<string, string> | undefined;
        if (s && attr in s) return { value: s[attr], quality: "Good", timestamp: new Date(0).toISOString() };
        return null;
      }
      default:
        return null;
    }
  }

  /**
   * A signed URL, stable within a one-minute bucket so a subscription does not see
   * a "change" on every read, and valid for 5–6 minutes from issue.
   */
  private href(m: MediaItem, now: number) {
    const verb = m.status === "available" ? "get" : "put";
    const bucket = Math.floor(now / 60_000) * 60_000;
    const { exp, sig } = this.d.media.sign(m.sha256, verb, 6 * 60_000, bucket);
    return `${this.d.publicBase()}/x-nexus/media/${m.sha256}?exp=${exp}&sig=${sig}`;
  }
}

function split(attributeId: string) {
  const i = attributeId.lastIndexOf("/");
  return { obj: attributeId.slice(0, i), attr: attributeId.slice(i + 1) };
}

function fieldOf(value: unknown, attr: string): VQT["value"] {
  if (value && typeof value === "object" && !Array.isArray(value)) return ((value as Record<string, unknown>)[attr] ?? null) as VQT["value"];
  return attr === "value" ? (value as VQT["value"]) : null;
}
