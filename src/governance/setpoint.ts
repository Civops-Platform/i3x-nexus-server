// Tier W2: setpoints (profile §7, Level 4).
//
// A setpoint is treated as one more system of record, so it goes through exactly
// the same governor lifecycle as a work order: request, approval, freshness check,
// conditional apply, read-back, automatic compensation. What is particular to W2
// lives here:
//
//   - The "record" is one bound attribute; its "version" is its current value and
//     quality, so a value that moved since the request makes the apply stale.
//   - The write envelope (range, largest single step, allowed states) is checked
//     at request time AND again at apply time, against the value then current. A
//     value outside it is rejected, never clamped.
//   - A tag that is not Good cannot be written: its version reads "not-good", which
//     never matches the version a request was raised against.
//   - The write goes out through the source driver's write path (OPC UA Write), the
//     one conduit into the plant network (NX-61).

import type { Binding, ObjectDef } from "../model/config";
import type { LastValueCache } from "../core/cache";
import { canonical } from "../core/records";
import type { ApplyResult, ExternalRecord, SystemAdapter } from "./adapters";

export interface Envelope {
  min?: number;
  max?: number;
  /** Largest change allowed in one write, relative to the value current when it is applied. */
  maxStep?: number;
  /** For discrete setpoints: the only values accepted. */
  allowed?: Array<string | number | boolean>;
}

export interface WritableDriver { write(address: string, value: unknown): Promise<{ ok: true } | { ok: false; detail: string; permanent?: boolean }> }

export const SETPOINT_SYSTEM = "plant";

/** Why a value falls outside an envelope, or null when it is inside. Pure. */
export function envelopeViolation(env: Envelope, proposed: unknown, current: unknown): string | null {
  if (env.allowed) return env.allowed.some((a) => a === proposed) ? null : `must be one of ${env.allowed.map(String).join(", ")}`;
  if (typeof proposed !== "number" || !Number.isFinite(proposed)) return "must be a finite number";
  if (env.min !== undefined && proposed < env.min) return `below the minimum ${env.min}`;
  if (env.max !== undefined && proposed > env.max) return `above the maximum ${env.max}`;
  if (env.maxStep !== undefined) {
    if (typeof current !== "number") return "the current value is not a number, so the step cannot be checked";
    if (Math.abs(proposed - current) > env.maxStep) return `a step of ${Math.abs(proposed - current)} exceeds the largest allowed step ${env.maxStep}`;
  }
  return null;
}

export class SetpointAdapter implements SystemAdapter {
  readonly id = SETPOINT_SYSTEM;
  constructor(
    private cache: LastValueCache,
    private bindings: Map<string, { binding: Binding & { envelope?: Envelope }; object: ObjectDef }>,
    private driverFor: (sourceId: string) => WritableDriver | null,
  ) {}

  /** attribute id → binding, for the W2 attributes only. */
  has(attributeId: string) { return this.bindings.has(attributeId); }
  envelopeOf(attributeId: string) { return this.bindings.get(attributeId)?.binding.envelope ?? null; }

  async list() { return []; }

  async read(attributeId: string): Promise<ExternalRecord | null> {
    if (!this.bindings.has(attributeId)) return null;
    const c = this.cache.get(attributeId);
    const good = c?.quality === "Good";
    return { id: attributeId, version: good ? `v:${canonical(c!.value)}` : "not-good", fields: { value: good ? c!.value : null } };
  }

  async apply(attributeId: string | null, fields: Record<string, unknown>, ifVersion: string | null, opts: { compensating?: boolean } = {}): Promise<ApplyResult> {
    const b = attributeId ? this.bindings.get(attributeId) : undefined;
    if (!attributeId || !b) return { ok: false, stale: false, status: 404, detail: "not a governed setpoint" };
    const cur = await this.read(attributeId);
    if (ifVersion !== null && cur!.version !== ifVersion) return { ok: false, stale: true, status: 409, detail: `value moved (${cur!.version})` };
    if (!opts.compensating) {
      const why = envelopeViolation(b.binding.envelope ?? {}, fields.value, cur!.fields.value);
      if (why) return { ok: false, stale: false, status: 422, detail: `outside the write envelope: ${why}` };
    }
    const d = this.driverFor(b.binding.source);
    if (!d) return { ok: false, stale: false, status: 501, detail: `source ${b.binding.source} has no write path` };
    const r = await d.write(b.binding.address, fields.value);
    // A failed write leaves an unknown outcome on some servers: report it as a system failure (retry), not a refusal.
    if (!r.ok) return { ok: false, stale: false, status: r.permanent ? 403 : 502, detail: r.detail };
    return { ok: true, record: { id: attributeId, version: "written", fields: { value: fields.value } } };
  }
}
