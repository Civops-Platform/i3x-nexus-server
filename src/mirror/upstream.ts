// The enterprise tier's view of one site nexus (profile NX-45, NX-48, §9).
//
// An enterprise nexus mirrors each site over plain i3X 1.0, the same contract any
// client uses: it reads the site's types and objects, subscribes to all of them,
// and holds the stream open. Nothing here needs the site to know it is being
// mirrored, so a site nexus from any vendor can be mounted.
//
// While the uplink is up, values arrive on the stream and go into the cache and
// the local history store. When it drops, every mirrored value goes Uncertain:
// the enterprise view shows the site's last values and says they are not live. It
// never shows them Good and never blanks them.
//
// On reconnect the gap since the outage began is replayed in the profile's order
// (NX-45): P0 elements first (alarms and quality: history, then current), then
// records (P2: their versions), then the latest P1 values, then P3, which is the
// realtime history backfill. Replay is pulled with ordinary i3X history and value
// reads, so the site serves it like any other client request, under its own load
// rules.

import type { Binding, ObjectDef, PriorityClass, TypeDef } from "../model/config";
import type { LastValueCache } from "../core/cache";
import type { HistoryStore } from "../core/history";
import type { NexusFabric } from "../i3x/fabric";
import type { VQT } from "../i3x/wire";

export interface UpstreamDef { id: string; url: string; token: string }

type Json = any;

const CHUNK = 500;

export class UpstreamMirror {
  connected = false;
  private abort: AbortController | null = null;
  private paused = false;
  private stopped = false;
  private downSince: number | null = null;
  private retryTimer: NodeJS.Timeout | null = null;
  private attrsOf = new Map<string, string[]>();   // object id → attribute names
  private scalar = new Set<string>();                // objects served as a scalar `value`
  private priority = new Map<string, PriorityClass>();
  /** What the last reconnect replayed, in order (for tests and /info). */
  replayLog: Array<{ phase: PriorityClass; elements: number; points: number }> = [];

  constructor(private up: UpstreamDef, private fabric: NexusFabric, private cache: LastValueCache, private history: () => HistoryStore) {}

  async start() { await this.connect(); }

  stop() {
    this.stopped = true;
    this.abort?.abort();
    if (this.retryTimer) clearTimeout(this.retryTimer);
  }

  /** Test hook: cut the uplink (and keep it cut) / restore it. */
  pause() { this.paused = true; this.abort?.abort(); }
  resume() { this.paused = false; void this.connect(); }

  private async call(method: string, path: string, body?: unknown): Promise<Json> {
    const r = await fetch(this.up.url + path, {
      method, signal: AbortSignal.timeout(20_000),
      headers: { authorization: `Bearer ${this.up.token}`, "content-type": "application/json" },
      body: body === undefined ? undefined : JSON.stringify(body),
    });
    if (!r.ok) throw new Error(`${method} ${path}: HTTP ${r.status}`);
    return r.json();
  }

  private scheduleRetry() {
    if (this.stopped || this.paused || this.retryTimer) return;
    this.retryTimer = setTimeout(() => { this.retryTimer = null; void this.connect(); }, 2_000);
  }

  private wentDown() {
    if (this.connected) {
      this.connected = false;
      this.downSince = Date.now();
      // Last values stay readable, marked not live.
      for (const [obj, attrs] of this.attrsOf) for (const a of attrs) this.cache.mark(`${obj}/${a}`, "Uncertain");
    }
    this.scheduleRetry();
  }

  private async connect() {
    if (this.stopped || this.paused) return;
    try {
      await this.loadModel();
      const since = this.downSince;
      await this.subscribeAndStream(since);
    } catch {
      this.wentDown();
    }
  }

  private async loadModel() {
    const [info, types, objects] = await Promise.all([
      this.call("GET", "/info"),
      this.call("GET", "/objecttypes"),
      this.call("GET", "/objects?includeMetadata=true"),
    ]);
    const byType: Record<string, PriorityClass> = info.result?.["x-nexus"]?.priorityByType ?? {};
    const tdefs: TypeDef[] = [];
    for (const t of types.result as Json[]) {
      const schema = t.schema ?? {};
      const isObj = schema.type === "object" && schema.properties;
      const attributes = isObj
        ? Object.entries(schema.properties as Record<string, Json>).map(([name, s]) => ({ name, dataType: jsonType(s.type) }))
        : [{ name: "value", dataType: jsonType(schema.type) }];
      tdefs.push({ id: t.elementId, name: t.displayName, namespaceUri: t.namespaceUri, kind: "mounted", attributes, priority: byType[t.elementId] ?? "P1" });
    }
    const tmap = new Map(tdefs.map((t) => [t.id, t]));
    const odefs: ObjectDef[] = [];
    this.attrsOf.clear(); this.scalar.clear(); this.priority.clear();
    for (const o of objects.result as Json[]) {
      const t = tmap.get(o.typeElementId);
      if (!t) continue;
      const rel: Record<string, string[]> = {};
      for (const [k, v] of Object.entries((o.metadata?.relationships ?? {}) as Record<string, string[]>)) {
        if (k !== "HasParent" && k !== "HasChildren") rel[k] = v;
      }
      const bindings: Record<string, Binding> = {};
      for (const a of t.attributes) {
        bindings[a.name] = { source: this.up.id, address: "" };
        this.cache.declare(`${o.elementId}/${a.name}`, this.up.id, 0);
      }
      odefs.push({ id: o.elementId, name: o.displayName, typeId: t.id, parentId: o.parentId ?? null, bindings, relationships: rel });
      this.attrsOf.set(o.elementId, t.attributes.map((a) => a.name));
      if (t.attributes.length === 1 && t.attributes[0].name === "value") this.scalar.add(o.elementId);
      this.priority.set(o.elementId, t.priority ?? "P1");
    }
    // Media and folder types are mirrored as references and structure; their bytes stay at the site.
    this.fabric.mount(this.up.id, tdefs, odefs);
  }

  private apply(u: { elementId: string; value: unknown; quality: VQT["quality"]; timestamp: string }, toHistory = true) {
    const attrs = this.attrsOf.get(u.elementId);
    if (!attrs) return;
    const pts: VQT[] = [];
    for (const a of attrs) {
      const v = this.scalar.has(u.elementId) ? u.value : (u.value && typeof u.value === "object" ? (u.value as Record<string, unknown>)[a] : null);
      const id = `${u.elementId}/${a}`;
      this.cache.set(id, (v ?? null) as VQT["value"], u.quality, u.timestamp);
      if (toHistory && v !== null && v !== undefined) pts.push({ attributeId: id, value: v as VQT["value"], quality: u.quality, timestamp: u.timestamp });
    }
    if (pts.length) this.history().append(pts);
  }

  private async replay(sinceMs: number) {
    this.replayLog = [];
    const from = new Date(sinceMs - 1_000).toISOString();
    const to = new Date().toISOString();
    const ids = [...this.attrsOf.keys()];
    const of = (c: PriorityClass) => ids.filter((id) => this.priority.get(id) === c);
    const historyOf = async (els: string[]) => {
      let points = 0;
      for (let i = 0; i < els.length; i += CHUNK) {
        const r = await this.call("POST", "/objects/history", { elementIds: els.slice(i, i + CHUNK), startTime: from, endTime: to });
        for (const row of r.results ?? []) {
          if (!row.success) continue;
          for (const v of row.result.values ?? []) { this.apply({ elementId: row.elementId, ...v }, true); points++; }
        }
      }
      return points;
    };
    const current = async (els: string[]) => {
      for (let i = 0; i < els.length; i += CHUNK) {
        const r = await this.call("POST", "/objects/value", { elementIds: els.slice(i, i + CHUNK) });
        for (const row of r.results ?? []) if (row.success) this.apply({ elementId: row.elementId, ...row.result }, false);
      }
    };
    const p0 = of("P0");
    this.replayLog.push({ phase: "P0", elements: p0.length, points: await historyOf(p0) });
    await current(p0);
    const p2 = of("P2");
    this.replayLog.push({ phase: "P2", elements: p2.length, points: await historyOf(p2) });
    await current(p2);
    const p1 = [...of("P1"), ...of("P3")];
    await current(p1);
    this.replayLog.push({ phase: "P1", elements: p1.length, points: 0 });
    this.replayLog.push({ phase: "P3", elements: p1.length, points: await historyOf(p1) });
  }

  private async subscribeAndStream(downSince: number | null) {
    const clientId = `mirror:${this.up.id}`;
    const ids = [...this.attrsOf.keys()];
    const sub = (await this.call("POST", "/subscriptions", { clientId, displayName: "enterprise mirror" })).result.subscriptionId;
    for (let i = 0; i < ids.length; i += 5_000) {
      await this.call("POST", "/subscriptions/register", { clientId, subscriptionId: sub, elementIds: ids.slice(i, i + 5_000), maxDepth: 1 });
    }
    this.abort = new AbortController();
    const r = await fetch(this.up.url + "/subscriptions/stream", {
      method: "POST", signal: this.abort.signal,
      headers: { authorization: `Bearer ${this.up.token}`, "content-type": "application/json", accept: "text/event-stream" },
      body: JSON.stringify({ clientId, subscriptionId: sub }),
    });
    if (!r.ok || !r.body) throw new Error(`stream: HTTP ${r.status}`);
    // Replay what was missed, then go live. Values arriving on the stream meanwhile are newer and win.
    if (downSince !== null) await this.replay(downSince);
    else await this.replayCurrent(ids);
    this.connected = true;
    this.downSince = null;
    const reader = r.body.getReader();
    const dec = new TextDecoder();
    let buf = "";
    void (async () => {
      try {
        for (;;) {
          const { value, done } = await reader.read();
          if (done) break;
          buf += dec.decode(value, { stream: true });
          let i;
          while ((i = buf.indexOf("\n\n")) >= 0) {
            const event = buf.slice(0, i);
            buf = buf.slice(i + 2);
            const data = event.split("\n").filter((l) => l.startsWith("data:")).map((l) => l.slice(5).trim()).join("");
            if (!data) continue;
            for (const u of JSON.parse(data) as Json[]) this.apply(u, true);
          }
        }
      } catch { /* aborted or dropped */ }
      // Best effort: free the site's subscription slot.
      void this.call("POST", "/subscriptions/delete", { clientId, subscriptionIds: [sub] }).catch(() => {});
      this.wentDown();
    })();
  }

  private async replayCurrent(ids: string[]) {
    for (let i = 0; i < ids.length; i += CHUNK) {
      const r = await this.call("POST", "/objects/value", { elementIds: ids.slice(i, i + CHUNK) });
      for (const row of r.results ?? []) if (row.success) this.apply({ elementId: row.elementId, ...row.result }, true);
    }
  }

  /** Forward a write batch to the site, with the caller's own credential (NX-48). */
  async forward(path: string, body: unknown, authorization: string | undefined, idempotencyKey?: string) {
    const r = await fetch(this.up.url + path, {
      method: "PUT", signal: AbortSignal.timeout(10_000),
      headers: { "content-type": "application/json", ...(authorization ? { authorization } : {}), ...(idempotencyKey ? { "idempotency-key": idempotencyKey } : {}) },
      body: JSON.stringify(body ?? {}),
    });
    return { status: r.status, text: await r.text(), written: r.headers.get("x-nexus-written") };
  }
}

function jsonType(t: unknown): TypeDef["attributes"][number]["dataType"] {
  const s = Array.isArray(t) ? t.find((x) => x !== "null") : t;
  return (["number", "integer", "string", "boolean", "object", "array"].includes(s as string) ? s : "string") as never;
}
