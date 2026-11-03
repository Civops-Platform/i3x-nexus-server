// The phase 1 write path: tier W0 only (spec §6.1).
//
// What each kind of object does with a PUT:
//   record   accepted if the principal owns the type's namespace; the value is
//            checked against the type, merged onto the current version, and stored
//            as a new version. A historical write adds a version at its timestamp.
//   intake   the media intake: needs the media:upload scope; creates a MediaItem.
//   realtime refused, 403. The value belongs to its source; changing it is a
//            setpoint write (tier W2), which does not exist before phase 4.
//   media    refused, 403. A MediaItem is defined by its bytes.
//   anything the principal does not own: refused, 403, naming what it would need.
//
// Ownership is checked against the type's namespace as this server holds it,
// never against anything the writer claims (a client-side ownership guard applied server-side).

import type { WriteOutcome } from "./wire";
import type { NexusFabric } from "./fabric";
import { MEDIA_INTAKE, mediaId } from "./fabric";
import type { AttributeDef } from "../model/config";
import { NotLeaderError, type RecordStore } from "../core/records";
import { validateMediaRequest, type MediaStore } from "../core/media";
import { can, owns, type Principal } from "../core/auth";

const RFC3339 = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:\d{2})$/i;

const JSON_OK: Record<AttributeDef["dataType"], (v: unknown) => boolean> = {
  number: (v) => typeof v === "number" && Number.isFinite(v),
  integer: (v) => Number.isInteger(v),
  string: (v) => typeof v === "string",
  boolean: (v) => typeof v === "boolean",
  object: (v) => !!v && typeof v === "object" && !Array.isArray(v),
  array: (v) => Array.isArray(v),
};

/** Merge `incoming` onto `base`, refusing unknown fields and wrong JSON types. */
export function mergeRecord(attrs: AttributeDef[], base: unknown, incoming: unknown): { ok: true; value: Record<string, unknown> } | { ok: false; detail: string } {
  if (!incoming || typeof incoming !== "object" || Array.isArray(incoming)) return { ok: false, detail: "value must be an object with this type's fields" };
  const known = new Map(attrs.map((a) => [a.name, a]));
  const out: Record<string, unknown> = { ...(base && typeof base === "object" ? (base as Record<string, unknown>) : {}) };
  for (const [k, v] of Object.entries(incoming as Record<string, unknown>)) {
    const a = known.get(k);
    if (!a) return { ok: false, detail: `unknown field ${k}; this type has ${[...known.keys()].join(", ")}` };
    if (v !== null && !JSON_OK[a.dataType](v)) return { ok: false, detail: `${k} must be ${a.dataType}` };
    if (v !== null && a.enum && !a.enum.includes(v as string)) return { ok: false, detail: `${k} must be one of ${a.enum.join(", ")}` };
    out[k] = v;
  }
  return { ok: true, value: out };
}

import type { Governor } from "../governance/governor";
import { W1_INTAKE, W2_INTAKE } from "./fabric";
import { LIMIT_TYPE } from "../governance/authority";

export interface WriteDeps {
  fabric: NexusFabric;
  records: RecordStore;
  media: MediaStore;
  /** From the request's Idempotency-Key header plus the item index: a retried batch applies once. */
  idempotencyKey?: string;
  /** Present when W1 is enabled (systems of record configured). */
  governor?: Governor | null;
}

export async function writeOne(d: WriteDeps, p: Principal, update: unknown, elementId: string, mode: "current" | "history"): Promise<WriteOutcome> {
  const u = update as { value?: { value?: unknown; quality?: unknown; timestamp?: unknown } } | null;
  const t = d.fabric.typeOf(elementId);
  if (!t) return { ok: false, status: 404, detail: `Element not found: ${elementId}` };

  if (elementId === W1_INTAKE || elementId === W2_INTAKE || t.id === "WriteRequest") {
    if (!d.governor) return { ok: false, status: 403, detail: "tier W1 is not enabled on this nexus (no system of record is configured)" };
    if (mode === "history") return { ok: false, status: 403, detail: "write requests take current writes only" };
    const v = (update as { value?: { value?: unknown } } | null)?.value?.value;
    try {
      const r = elementId === W1_INTAKE ? await d.governor.request(p, v, "W1")
        : elementId === W2_INTAKE ? await d.governor.request(p, v, "W2")
        : await d.governor.decide(p, elementId, v);
      return r.ok ? { ok: true, id: r.id } : { ok: false, status: r.status, detail: r.detail };
    } catch (e) {
      if (e instanceof NotLeaderError) return { ok: false, status: 503, detail: `write leader changed during this write; retry (${e.message})` };
      throw e;
    }
  }
  if (t.kind === "external") {
    return { ok: false, status: 403, detail: `${elementId} is a record in a system of record. Change it through a WriteRequest: PUT ${W1_INTAKE} (NX-22)` };
  }
  if (t.kind === "mounted") {
    return { ok: false, status: 403, detail: `${elementId} belongs to an upstream site nexus; write it there (this nexus forwards whole batches for one site)` };
  }
  if (t.kind === "realtime" && t.write === "W2") {
    return { ok: false, status: 403, detail: `${elementId} is a governed setpoint. Change it through a WriteRequest: PUT ${W2_INTAKE} (NX-22)` };
  }
  if (t.kind === "realtime") {
    return { ok: false, status: 403, detail: `${elementId} is a realtime value owned by its source. Changing it is a setpoint write (tier W2), which this nexus does not accept before phase 4.` };
  }
  if (t.kind === "media" || t.kind === "folder" || t.kind === "stream") {
    return { ok: false, status: 403, detail: `${elementId} is read-only (${t.kind}). New media goes through ${MEDIA_INTAKE}.` };
  }
  if (!u?.value || typeof u.value !== "object" || !("value" in u.value)) return { ok: false, status: 400, detail: "updates[].value.value is required" };
  const ts = u.value.timestamp;
  if (ts !== undefined && (typeof ts !== "string" || !RFC3339.test(ts) || Number.isNaN(Date.parse(ts)))) {
    return { ok: false, status: 400, detail: "timestamp must be RFC 3339" };
  }
  if (mode === "history" && ts === undefined) return { ok: false, status: 400, detail: "a historical write needs value.timestamp" };
  const timestamp = ts === undefined ? undefined : new Date(ts as string).toISOString();

  if (elementId === MEDIA_INTAKE) {
    if (!can(p, "media:upload")) return { ok: false, status: 403, detail: "requesting a media upload needs the media:upload scope" };
    if (mode === "history") return { ok: false, status: 403, detail: "the media intake takes current writes only" };
    const r = validateMediaRequest(u.value.value, d.media.maxBytes);
    if (!r.ok) return { ok: false, status: 400, detail: r.detail };
    for (const ref of [r.req.capturedBy, r.req.documents]) {
      if (ref && !d.fabric.objectDef(ref)) return { ok: false, status: 400, detail: `${ref} is not an object on this nexus` };
    }
    const before = d.media.get(r.req.sha256);
    await d.media.request(r.req, p.id);
    if (!before) d.fabric.rebuild();
    return { ok: true, id: mediaId(r.req.sha256) };
  }

  if (t.write !== "W0") return { ok: false, status: 403, detail: `${elementId} is not writable` };
  if (t.id === LIMIT_TYPE) {
    // NX-28: a limit is never changed by a plain write, not even by the namespace owner.
    return { ok: false, status: 403, detail: d.governor
      ? `${elementId} is an authority limit. Change it through a WriteRequest naming it as the target: PUT ${W1_INTAKE}; it is approved one level above the limit's owner (NX-28)`
      : `${elementId} is an authority limit; this nexus has no write governor, so limits change only in the site config (NX-28)` };
  }
  if (!owns(p, t.namespaceUri)) {
    return { ok: false, status: 403, detail: `principal ${p.id} does not own ${t.namespaceUri}; a W0 write needs ownership of the type's namespace` };
  }
  const base = mode === "history"
    ? d.records.history(elementId).find((v) => v.timestamp === timestamp)?.value ?? d.records.current(elementId)?.value
    : d.records.current(elementId)?.value;
  const m = mergeRecord(t.attributes, base, u.value.value);
  if (!m.ok) return { ok: false, status: 400, detail: m.detail };
  try {
    const v = await d.records.write(elementId, m.value, p.id, timestamp, d.idempotencyKey ? `${p.id}:${d.idempotencyKey}` : undefined);
    return { ok: true, id: `${elementId}@${v.version}` };
  } catch (e) {
    if (e instanceof NotLeaderError) return { ok: false, status: 503, detail: `write leader changed during this write; retry (${e.message})` };
    throw e;
  }
}
