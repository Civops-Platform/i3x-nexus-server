// The site model: what the nexus serves and where each value comes from.
//
// A site is described in one JSON file (config/*.json). Types say what an object
// is and how it behaves under load and on write; objects say where they sit in the
// hierarchy and, for realtime ones, which source feeds each attribute. Nothing in
// here is computed: the file is the reviewed statement of what this site exposes.

import { readFileSync } from "node:fs";

/** Spec §8.1. Assigned by the server from the type; a caller never raises it. */
export type PriorityClass = "P0" | "P1" | "P2" | "P3";

/**
 * How an object's value is held, spec §5:
 *   realtime  last value from a source, history from the event log / historian
 *   record    a transactional record owned by the nexus; history is its versions
 *   media     a MediaItem reference; bytes live in the media store (§5.4)
 *   stream    a MediaStream descriptor; bytes never pass through i3X
 */
export type ObjectKind = "realtime" | "record" | "media" | "stream" | "folder" | "mounted" | "external";

/** Spec §6.1. W1 and W2 are phase 3 and 4; phase 1 accepts W0 only. */
export type WriteTier = "none" | "W0" | "W1" | "W2";

export interface AttributeDef {
  name: string;
  dataType: "number" | "integer" | "string" | "boolean" | "object" | "array";
  unit?: string;
  enum?: string[];
}

export interface TypeDef {
  id: string;
  name: string;
  namespaceUri: string;
  kind: ObjectKind;
  attributes: AttributeDef[];
  priority?: PriorityClass;
  write?: WriteTier;
}

export interface Binding {
  /** A source id from `sources`. */
  source: string;
  /** Driver-specific: an OPC UA NodeId, an MQTT topic, a simulator expression. */
  address: string;
  /**
   * Expected update period, for sources that publish on a fixed cycle. A value older
   * than 3× this reads Uncertain (§5). Leave it out for report-by-exception sources,
   * where silence means "unchanged", not "stale".
   */
  scanMs?: number;
  /** Where this attribute's long history lives, if not in the nexus (profile §5 federation). */
  history?: { historian: string; address: string };
  /** W2 only: the write envelope. A W2 attribute without one cannot be written. */
  envelope?: import("../governance/setpoint").Envelope;
}

export interface ObjectDef {
  id: string;
  name: string;
  typeId: string;
  parentId?: string | null;
  /** realtime objects: attribute name → binding. */
  bindings?: Record<string, Binding>;
  /** Non-hierarchical relationships declared in the model: { CapturedBy: [id] }. */
  relationships?: Record<string, string[]>;
  /** stream objects: the static part of the descriptor. */
  stream?: {
    codec: string; resolution: string;
    /** Static viewer URL, when no gateway is configured. With a gateway it is signed per viewer. */
    viewerUrl?: string;
    /** Gateway path name, and the device's own RTSP URL the gateway pulls. */
    path?: string;
    cameraUrl?: string;
  };
  /** record objects: the value written as version 1 (principal `nexus:config`) if none exists. */
  initial?: Record<string, unknown>;
}

export interface SourceDef {
  id: string;
  driver: "simulator" | "opcua" | "mqtt" | "sparkplug" | "mediamtx";
  /** opc.tcp://… or mqtt://… ; unused by the simulator. */
  endpoint?: string;
  options?: Record<string, unknown>;
}

export interface NamespaceDef { uri: string; name: string; version: string }

export interface SiteConfig {
  site: { id: string; name: string };
  namespaces: NamespaceDef[];
  types: TypeDef[];
  objects: ObjectDef[];
  sources: SourceDef[];
  /**
   * Enterprise tier (profile NX-45, NX-48): site nexuses mirrored into this one.
   * The token is read from the named environment variable, never from this file.
   */
  upstreams?: Array<{ id: string; url: string; tokenEnv: string }>;
  /**
   * Systems of record behind tier W1. Their records are projected into the address
   * space (read-only, kind "external") and changed only through WriteRequests.
   */
  /** Media gateway for live streams (profile §6, NX-15). */
  mediaGateway?: import("../media/gateway").GatewayConfig;
  /** Cut a clip from a stream's recording when an alarm trips; stored as a MediaItem. */
  clips?: Array<{ trigger: string; stream: string; preS: number; postS: number }>;
  /** Plant historians the nexus reads history from instead of keeping its own (federation). */
  historians?: Array<{ id: string; kind: "ignition-sql"; urlEnv: string; provider: string; dialect?: "postgres" | "mysql" | "mssql" }>;
  systems?: Array<{ id: string; kind: "cmms-rest"; url: string; tokenEnv?: string; typeId: string; name?: string }>;
  /** Relationship pairs beyond HasParent/HasChildren, each registered both ways. */
  relationshipTypes?: Array<[string, string]>;
}

export function loadConfig(path: string): SiteConfig {
  const cfg = JSON.parse(readFileSync(path, "utf8")) as SiteConfig;
  const problems = validateConfig(cfg);
  if (problems.length) throw new Error(`Invalid site config ${path}:\n  - ${problems.join("\n  - ")}`);
  return cfg;
}

/** Every structural mistake at once, so a config is fixed in one pass, not one error per restart. */
export function validateConfig(cfg: SiteConfig): string[] {
  const out: string[] = [];
  const ns = new Set(cfg.namespaces.map((n) => n.uri));
  const types = new Map(cfg.types.map((t) => [t.id, t]));
  const sources = new Set(cfg.sources.map((s) => s.id));
  const ids = new Set<string>();
  for (const t of cfg.types) {
    if (!ns.has(t.namespaceUri)) out.push(`type ${t.id}: namespace ${t.namespaceUri} is not declared`);
    if (t.write === "W2" && t.kind !== "realtime") out.push(`type ${t.id}: W2 applies to realtime (setpoint) types only`);
    if (t.write === "W1" && t.kind !== "external") out.push(`type ${t.id}: W1 applies to external (system-of-record) types only`);
    if (t.write === "W0" && t.kind === "realtime") out.push(`type ${t.id}: a realtime value cannot be W0; it belongs to its source`);
  }
  for (const o of cfg.objects) {
    if (ids.has(o.id)) out.push(`object ${o.id}: duplicate id`);
    ids.add(o.id);
    const t = types.get(o.typeId);
    if (!t) { out.push(`object ${o.id}: unknown type ${o.typeId}`); continue; }
    if (t.kind === "realtime") {
      for (const a of t.attributes) {
        const b = o.bindings?.[a.name];
        if (!b) out.push(`object ${o.id}: attribute ${a.name} has no binding`);
        else if (!sources.has(b.source)) out.push(`object ${o.id}: binding ${a.name} names unknown source ${b.source}`);
      }
    }
    if (t.write === "W2") {
      for (const [a, b] of Object.entries(o.bindings ?? {})) {
        const src = cfg.sources.find((s) => s.id === b.source);
        if (!b.envelope) out.push(`object ${o.id}: W2 attribute ${a} has no write envelope`);
        if (src && src.driver !== "opcua") out.push(`object ${o.id}: W2 attribute ${a} is on a ${src.driver} source; only OPC UA has a write path`);
      }
    }
    if (t.kind === "stream" && !o.stream) out.push(`object ${o.id}: stream object without a stream descriptor`);
    if (t.kind === "stream" && o.stream && !o.stream.viewerUrl && !(o.stream.path && cfg.mediaGateway)) out.push(`object ${o.id}: stream needs a viewerUrl, or a path and a mediaGateway`);
  }
  for (const o of cfg.objects) {
    if (o.parentId && !ids.has(o.parentId)) out.push(`object ${o.id}: parent ${o.parentId} does not exist`);
    for (const [rel, targets] of Object.entries(o.relationships ?? {})) {
      if (!(cfg.relationshipTypes ?? []).some(([a, b]) => a === rel || b === rel)) out.push(`object ${o.id}: relationship ${rel} is not registered`);
      for (const tid of targets) if (!ids.has(tid)) out.push(`object ${o.id}: ${rel} target ${tid} does not exist`);
    }
  }
  const hist = new Set((cfg.historians ?? []).map((h) => h.id));
  for (const o of cfg.objects) for (const [a, b] of Object.entries(o.bindings ?? {})) {
    if (b.history && !hist.has(b.history.historian)) out.push(`object ${o.id}: binding ${a} names unknown historian ${b.history.historian}`);
  }
  for (const s of cfg.systems ?? []) {
    const t = types.get(s.typeId);
    if (!t) out.push(`system ${s.id}: unknown type ${s.typeId}`);
    else if (t.kind !== "external") out.push(`system ${s.id}: type ${s.typeId} must be kind "external"`);
  }
  return out;
}
