// MQTT / UNS source. An address is a topic, optionally with a JSON field:
//
//   site1/line2/filler/speed            payload is the value itself, or JSON
//   site1/line2/filler/state#value      JSON payload, take field `value`
//
// A JSON payload may carry { value, timestamp, quality }; without a timestamp the
// receive time is used and the quality is Uncertain, because a value whose time is
// unknown is not a Good reading. Sparkplug B (protobuf, birth/death certificates)
// is the next driver and is not built yet.
//
// Broker loss marks every attribute Bad; reconnects are mqtt.js's own, every 2 s.

import mqtt, { type MqttClient } from "mqtt";
import type { Binding, SourceDef } from "../model/config";
import type { Quality } from "../core/cache";
import type { Driver, DriverSink } from "./driver";

const QUALITIES = new Set(["Good", "Uncertain", "Bad", "GoodNoData"]);

export function parsePayload(raw: Buffer, field: string | null, receivedAt: string): { value: unknown; quality: Quality; timestamp: string } | null {
  const text = raw.toString("utf8").trim();
  let parsed: unknown = text;
  try { parsed = JSON.parse(text); } catch { /* a bare string payload */ }
  if (field) {
    if (!parsed || typeof parsed !== "object") return null;
    const doc = parsed as Record<string, unknown>;
    if (!(field in doc)) return null;
    const ts = typeof doc.timestamp === "string" && !Number.isNaN(Date.parse(doc.timestamp)) ? new Date(doc.timestamp).toISOString() : null;
    const q = typeof doc.quality === "string" && QUALITIES.has(doc.quality) ? (doc.quality as Quality) : null;
    return { value: doc[field], quality: q ?? (ts ? "Good" : "Uncertain"), timestamp: ts ?? receivedAt };
  }
  if (parsed && typeof parsed === "object" && !Array.isArray(parsed) && "value" in (parsed as object)) {
    return parsePayload(raw, "value", receivedAt);
  }
  return { value: parsed, quality: "Uncertain", timestamp: receivedAt };
}

export class MqttDriver implements Driver {
  private client: MqttClient | null = null;
  private connected = false;
  private everConnected = false;
  private detail = "not started";
  constructor(readonly source: SourceDef) {}

  async start(points: Array<{ attributeId: string; binding: Binding }>, sink: DriverSink) {
    const byTopic = new Map<string, Array<{ attributeId: string; field: string | null }>>();
    for (const { attributeId, binding } of points) {
      const [topic, field] = binding.address.split("#");
      if (!byTopic.has(topic)) byTopic.set(topic, []);
      byTopic.get(topic)!.push({ attributeId, field: field ?? null });
    }
    const opts = (this.source.options ?? {}) as Record<string, unknown>;
    this.client = mqtt.connect(this.source.endpoint!, { reconnectPeriod: 2000, connectTimeout: 5000, ...opts });
    this.client.on("connect", () => {
      this.connected = true;
      this.everConnected = true;
      this.detail = "connected";
      this.client!.subscribe([...byTopic.keys()], { qos: 1 });
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
      const now = new Date().toISOString();
      for (const it of byTopic.get(topic) ?? []) {
        const v = parsePayload(payload, it.field, now);
        if (v) sink.value(it.attributeId, v.value as never, v.quality, v.timestamp);
      }
    });
    // A source that never connects must still read Bad, not GoodNoData forever.
    setTimeout(() => { if (!this.everConnected) sink.lost(`no connection to ${this.source.endpoint} after 5 s`); }, 5000).unref();
  }

  async stop() {
    const c = this.client;
    this.client = null;
    if (c) await new Promise<void>((r) => c.end(true, {}, () => r()));
  }

  status() { return { connected: this.connected, detail: this.detail }; }
}
