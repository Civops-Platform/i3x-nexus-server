// Sparkplug B (Eclipse Sparkplug 3.0) source, as a host application that only
// consumes. Addresses:
//
//   <group>/<edge node>#<metric name>                 a node metric
//   <group>/<edge node>/<device>#<metric name>        a device metric
//
// What Sparkplug adds over plain MQTT, and how each piece maps (spec §5):
//
//   BIRTH   declares every metric with name, alias, type and value. DATA after it
//           may carry the alias only, so the alias table is per node/device and
//           is rebuilt on every BIRTH.
//   DEATH   the edge node or device is gone. Every metric it births goes Bad.
//           An NDEATH whose bdSeq does not match the current birth is stale (an
//           old session's will) and is ignored, as the spec requires.
//   seq     0–255 per edge node, across NDATA/DDATA/DBIRTH/DDEATH. A gap means
//           messages were lost; the node's metrics read Uncertain until its next
//           BIRTH, because values may have changed that we never saw.
//   Quality metric property (Int32): 192 Good, 500 stale → Uncertain, 0 Bad.
//   is_null → value null, quality Bad.
//
// Rebirth requests (NCMD "Node Control/Rebirth") write to the plant network, so
// they are OFF unless the source sets `options.requestRebirth: true` (spec NX-61:
// the only write path into the plant network is an explicit, declared one).

import mqtt, { type MqttClient } from "mqtt";
import { get as sparkplug } from "sparkplug-payload";
import type { Binding, SourceDef } from "../model/config";
import type { Quality } from "../core/cache";
import type { Driver, DriverSink } from "./driver";

const sp = sparkplug("spBv1.0")!;

type Long = { low: number; high: number; unsigned?: boolean };
const isLong = (v: unknown): v is Long => !!v && typeof v === "object" && "low" in (v as object) && "high" in (v as object);
export const num = (v: unknown): number | null => {
  if (v === null || v === undefined) return null;
  if (typeof v === "number") return v;
  if (typeof v === "bigint") return Number(v);
  if (isLong(v)) return v.high * 2 ** 32 + (v.low >>> 0);
  return null;
};

export interface SpMetric {
  name?: string;
  alias?: unknown;
  type?: string;
  value?: unknown;
  timestamp?: unknown;
  isNull?: boolean;
  is_null?: boolean;
  properties?: Record<string, { value?: unknown }>;
}

export interface SpTopic { group: string; kind: string; node: string; device: string | null }

export function parseTopic(topic: string): SpTopic | null {
  const p = topic.split("/");
  if (p[0] !== "spBv1.0" || p.length < 4) return null;
  return { group: p[1], kind: p[2], node: p[3], device: p[4] ?? null };
}

export function metricValue(m: SpMetric): unknown {
  if (m.isNull || m.is_null) return null;
  const v = m.value;
  if (isLong(v)) return num(v);
  if (typeof v === "bigint") return Number(v);
  if (v instanceof Date) return v.toISOString();
  if (v && typeof v === "object" && !Array.isArray(v)) return null; // DataSet / Template: not a scalar; served as null, never guessed
  return v ?? null;
}

export function metricQuality(m: SpMetric): Quality {
  if (m.isNull || m.is_null) return "Bad";
  const q = num(m.properties?.Quality?.value);
  if (q === null || q === 192) return "Good";
  if (q === 500) return "Uncertain";
  if (q === 0) return "Bad";
  return q >= 192 ? "Good" : q >= 64 ? "Uncertain" : "Bad";
}

interface Scope { alias: Map<number, string>; birthed: boolean }

export class SparkplugDriver implements Driver {
  private client: MqttClient | null = null;
  private connected = false;
  private everConnected = false;
  private detail = "not started";
  private scopes = new Map<string, Scope>();          // "group/node[/device]" → aliases
  private seq = new Map<string, number>();            // "group/node" → last seq
  private bdSeq = new Map<string, number>();          // "group/node" → bdSeq of the live birth
  /** "group/node[/device]#metric" → attribute ids bound to it */
  private bound = new Map<string, string[]>();
  constructor(readonly source: SourceDef) {}

  async start(points: Array<{ attributeId: string; binding: Binding }>, sink: DriverSink) {
    const groups = new Set<string>();
    for (const { attributeId, binding } of points) {
      const [scope, metric] = binding.address.split("#");
      if (!metric) throw new Error(`Sparkplug address needs #<metric>: ${binding.address}`);
      groups.add(scope.split("/")[0]);
      const key = `${scope}#${metric}`;
      this.bound.set(key, [...(this.bound.get(key) ?? []), attributeId]);
    }
    const opts = (this.source.options ?? {}) as Record<string, unknown>;
    const { requestRebirth: _r, ...mqttOpts } = opts;
    this.client = mqtt.connect(this.source.endpoint!, { reconnectPeriod: 2000, connectTimeout: 5000, ...mqttOpts });
    this.client.on("connect", () => {
      this.connected = true;
      this.everConnected = true;
      this.detail = "connected";
      this.client!.subscribe([...groups].map((g) => `spBv1.0/${g}/#`), { qos: 1 });
      // Values from before a reconnect may be stale; births re-establish them.
      sink.restored();
    });
    const down = (why: string) => {
      if (this.connected) sink.lost(why);
      this.connected = false;
      this.detail = why;
    };
    this.client.on("close", () => down("broker connection closed"));
    this.client.on("offline", () => down("broker offline"));
    this.client.on("error", (e) => { this.detail = e.message; });
    this.client.on("message", (topic, payload) => {
      try { this.onMessage(topic, payload, sink); } catch { /* a malformed payload is dropped, never half-applied */ }
    });
    setTimeout(() => { if (!this.everConnected) sink.lost(`no connection to ${this.source.endpoint} after 5 s`); }, 5000).unref();
  }

  private onMessage(topic: string, payload: Buffer, sink: DriverSink) {
    const t = parseTopic(topic);
    if (!t) return;
    const nodeKey = `${t.group}/${t.node}`;
    const scopeKey = t.device ? `${nodeKey}/${t.device}` : nodeKey;
    const p = sp.decodePayload(payload) as { timestamp?: unknown; seq?: unknown; metrics?: SpMetric[] };
    const metrics = p.metrics ?? [];
    const payloadTs = num(p.timestamp);

    switch (t.kind) {
      case "NBIRTH": {
        const bd = num(metrics.find((m) => m.name === "bdSeq")?.value);
        if (bd !== null) this.bdSeq.set(nodeKey, bd);
        this.seq.set(nodeKey, num(p.seq) ?? 0);
        // A node birth invalidates its devices' births too.
        for (const k of this.scopes.keys()) if (k.startsWith(`${nodeKey}/`)) this.scopes.delete(k);
        this.birth(scopeKey, metrics, payloadTs, sink);
        return;
      }
      case "DBIRTH":
        if (!this.checkSeq(nodeKey, p.seq, sink)) return;
        this.birth(scopeKey, metrics, payloadTs, sink);
        return;
      case "NDATA":
      case "DDATA": {
        if (!this.checkSeq(nodeKey, p.seq, sink)) return;
        const s = this.scopes.get(scopeKey);
        if (!s?.birthed) { this.markScope(scopeKey, "Uncertain", sink); this.rebirth(t); return; }
        for (const m of metrics) this.emit(scopeKey, s, m, payloadTs, sink);
        return;
      }
      case "NDEATH": {
        const bd = num(metrics.find((m) => m.name === "bdSeq")?.value);
        if (bd !== null && this.bdSeq.has(nodeKey) && bd !== this.bdSeq.get(nodeKey)) return; // a stale will
        this.markNode(nodeKey, sink);
        return;
      }
      case "DDEATH":
        if (!this.checkSeq(nodeKey, p.seq, sink)) return;
        this.markScope(scopeKey, "Bad", sink);
        this.scopes.delete(scopeKey);
        return;
    }
  }

  private birth(scopeKey: string, metrics: SpMetric[], payloadTs: number | null, sink: DriverSink) {
    const s: Scope = { alias: new Map(), birthed: true };
    for (const m of metrics) {
      const a = num(m.alias);
      if (a !== null && m.name) s.alias.set(a, m.name);
    }
    this.scopes.set(scopeKey, s);
    for (const m of metrics) this.emit(scopeKey, s, m, payloadTs, sink);
  }

  private emit(scopeKey: string, s: Scope, m: SpMetric, payloadTs: number | null, sink: DriverSink) {
    const name = m.name ?? (num(m.alias) !== null ? s.alias.get(num(m.alias)!) : undefined);
    if (!name) return;
    const ids = this.bound.get(`${scopeKey}#${name}`);
    if (!ids) return;
    const ms = num(m.timestamp) ?? payloadTs;
    const ts = new Date(ms ?? Date.now()).toISOString();
    const q = ms === null ? "Uncertain" : metricQuality(m);
    for (const id of ids) sink.value(id, metricValue(m) as never, q, ts);
  }

  /** seq must advance by one (mod 256). A gap: everything from this node is doubtful until rebirth. */
  private checkSeq(nodeKey: string, raw: unknown, sink: DriverSink): boolean {
    const got = num(raw);
    const last = this.seq.get(nodeKey);
    if (got === null || last === undefined) return true;
    this.seq.set(nodeKey, got);
    if (got === (last + 1) % 256) return true;
    for (const k of [...this.scopes.keys()]) if (k === nodeKey || k.startsWith(`${nodeKey}/`)) { this.markScope(k, "Uncertain", sink); this.scopes.get(k)!.birthed = false; }
    const [group, node] = nodeKey.split("/");
    this.rebirth({ group, node, kind: "", device: null });
    return false;
  }

  private markNode(nodeKey: string, sink: DriverSink) {
    for (const [key, ids] of this.bound) {
      const scope = key.split("#")[0];
      if (scope === nodeKey || scope.startsWith(`${nodeKey}/`)) for (const id of ids) sink.value(id, null, "Bad", new Date().toISOString());
    }
    for (const k of [...this.scopes.keys()]) if (k === nodeKey || k.startsWith(`${nodeKey}/`)) this.scopes.delete(k);
  }

  private markScope(scopeKey: string, q: Quality, sink: DriverSink) {
    for (const [key, ids] of this.bound) {
      if (key.split("#")[0] !== scopeKey) continue;
      for (const id of ids) {
        if (sink.degrade) sink.degrade(id, q);
        else sink.value(id, null, q, new Date().toISOString());
      }
    }
  }

  private rebirth(t: SpTopic) {
    if (!(this.source.options as { requestRebirth?: boolean } | undefined)?.requestRebirth || !this.client) return;
    const body = sp.encodePayload({ timestamp: Date.now(), metrics: [{ name: "Node Control/Rebirth", type: "Boolean", value: true }] });
    this.client.publish(`spBv1.0/${t.group}/NCMD/${t.node}`, body as Buffer, { qos: 0 });
  }

  async stop() {
    const c = this.client;
    this.client = null;
    if (c) await new Promise<void>((r) => c.end(true, {}, () => r()));
  }

  status() { return { connected: this.connected, detail: this.detail }; }
}
