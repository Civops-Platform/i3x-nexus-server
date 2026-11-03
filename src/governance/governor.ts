// The write governor for tier W1 (profile §7).
//
// Every W1 write is a WriteRequest: a record in the nexus ledger whose versions ARE
// its provenance. Each transition (requested, an approval, approved, applied,
// verified, rejected, expired, rolled_back) is one new version, hash-chained with
// everything else the nexus has written, so the request's i3X history is its audit
// trail (NX-30..32).
//
// Nothing here is a model call. The governor computes status and escalation itself
// and ignores whatever a writer says about either (NX-23). The checks before apply
// are the profile's §7.4, in order: identity, authority, freshness, approval. Apply
// is conditional on the version the request was raised against (If-Match), and the
// result is read back: a mismatch is compensated at once, with no approval needed,
// because the approved state never existed (NX-34).
//
// Only the governor lease holder runs this. A request in flight when a governor
// dies is picked up by the next one: `resume()` re-reads the system of record and
// either records the apply that already happened or makes it, never both.

import { createHash, createPublicKey, verify as cryptoVerify } from "node:crypto";
import type { Principal } from "../core/auth";
import type { RecordStore, RecordVersion } from "../core/records";
import { AUTHORITY_SYSTEM, approverRolesFor, escalationOf, limitChangeProblem, parseLimit, type AuthorityLimit, type Escalation } from "./authority";
import type { ExternalRecord, SystemAdapter } from "./adapters";

export const W1_INTAKE = "nexus:intake:W1";
export const W2_INTAKE = "nexus:intake:W2";
export const WR_PREFIX = "nexus:wr:";
/** What an approver signs (NXA-6): the request, the decision, and the version they read. */
export const approvalSigningString = (id: string, decision: "approve" | "reject", versionHash: string) => `nexus-approval:v1|${id}|${decision}|${versionHash}`;
export const wrIdFor = (principal: string, key: string) => WR_PREFIX + createHash("sha256").update(`${principal}\n${key}`).digest("hex").slice(0, 24);

export type WrStatus = "requested" | "approved" | "applied" | "verified" | "rejected" | "expired" | "rolled_back";

export interface WriteRequestValue {
  target: { elementId: string; system: string; recordId: string | null };
  tier: "W1" | "W2";
  proposed: Record<string, unknown>;
  prior: { value: Record<string, unknown> | null; version: string | null; timestamp: string };
  /** For an agent on a grant, onBehalfOf is the grant's sponsor, never what the agent claims (NXA-2). */
  actor: { principal: string; kind: Principal["kind"]; onBehalfOf: string | null; grant?: { id: string; task: string } | null };
  justification: { decisionRef: string | null; falsification: unknown };
  matter: { site: string; exceptionType: string; exposureUsd: number | null; boundaryTriggers: string[] };
  authority: { limitRef: string | null; escalation: Escalation; approverRoles: string[] | null };
  reversible: boolean;
  compensation: { proposed: Record<string, unknown> } | null;
  compensates: string | null;
  idempotencyKey: string;
  status: WrStatus;
  statusDetail: string | null;
  approvals: Array<{ principal: string; role: string; decision: "approve" | "reject"; comment: string | null; at: string; signature?: { signedHash: string; sig: string } | null }>;
  applied: { at: string; ack: string } | null;
  verification: { readBack: Record<string, unknown> | null; match: boolean; at: string } | null;
  expiresAt: string;
}

export type GovResult = { ok: true; id: string; value: WriteRequestValue } | { ok: false; status: number; detail: string };

export interface GovernorDeps {
  records: () => RecordStore;
  site: string;
  /** External record element id → its system and record id; null if the element is not external. */
  resolveTarget: (elementId: string) => { system: string; recordId: string | null } | null;
  /** W2: a setpoint attribute behind an object (elementId + field), or null. */
  resolveSetpoint?: (elementId: string, field: string) => { system: string; recordId: string } | null;
  /** W2: its write envelope check against the value now current; null when inside. */
  envelopeCheck?: (attributeId: string, proposed: unknown, current: unknown) => string | null;
  /** W2 is suspended while this returns a reason (degraded redundancy, NX-53). */
  w2Suspended?: () => string | null;
  /** Unapproved W2 requests expire sooner (profile §12: 15 min). */
  w2ExpiryMs?: number;
  adapters: Map<string, SystemAdapter>;
  /** Every AuthorityLimit on this nexus: element id → current value. */
  limits: () => Array<{ elementId: string; value: unknown }>;
  /** Requests expire unapproved after this (NX profile §12: W1 4 h). */
  expiryMs?: number;
  verifyWindowMs?: number;
  /** Agent extension: an agent's request inside its grant? Null = yes; `used` = requests already raised on the grant. */
  agentCheck?: (p: Principal, tier: "W1" | "W2", targetId: string, used: number) => string | null;
  /** Agent extension: non-null while agent writes are frozen; approved agent requests are held, not applied. */
  agentWritesFrozen?: () => string | null;
  /** Agent extension: every approval is signed with the approver's registered key (NXA-6). */
  requireSignedApprovals?: boolean;
  now?: () => number;
  onChange?: () => void;
}

const SYSTEM = "nexus:governor";
const same = (a: unknown, b: unknown) =>
  typeof a === "number" && typeof b === "number" ? Math.abs(a - b) <= 1e-6 * Math.max(1, Math.abs(a), Math.abs(b)) : JSON.stringify(a ?? null) === JSON.stringify(b ?? null);
const sameFields = (want: Record<string, unknown>, got: Record<string, unknown> | null) =>
  !!got && Object.entries(want).every(([k, v]) => same(got[k], v));
const pick = (fields: Record<string, unknown>, keys: string[]) => Object.fromEntries(keys.map((k) => [k, fields[k] ?? null]));

export class Governor {
  private busy = new Set<string>();
  constructor(private d: GovernorDeps) {}

  private now() { return this.d.now?.() ?? Date.now(); }
  private iso() { return new Date(this.now()).toISOString(); }
  current(id: string): WriteRequestValue | null { return (this.d.records().current(id)?.value as WriteRequestValue) ?? null; }
  ids() { return this.d.records().ids().filter((i) => i.startsWith(WR_PREFIX)); }

  private async save(id: string, v: WriteRequestValue, who: string, key: string) {
    // Each transition gets its own idempotency key, so a governor retrying after a
    // failover cannot record the same transition twice.
    const rv: RecordVersion = await this.d.records().write(id, v, who, undefined, `${id}:${key}`);
    this.d.onChange?.();
    return rv;
  }

  private findLimit(exceptionType: string): { elementId: string; limit: AuthorityLimit } | null {
    for (const l of this.d.limits()) {
      const lim = parseLimit(l.value);
      if (lim && lim.site === this.d.site && lim.exceptionType === exceptionType) return { elementId: l.elementId, limit: lim };
    }
    return null;
  }

  /** PUT on the W1 or W2 intake. */
  async request(p: Principal, input: unknown, tier: "W1" | "W2" = "W1"): Promise<GovResult> {
    const i = input as Record<string, any>;
    if (!i || typeof i !== "object") return { ok: false, status: 400, detail: "value must be a WriteRequest object" };
    if (typeof i.idempotencyKey !== "string" || !i.idempotencyKey) return { ok: false, status: 400, detail: "idempotencyKey is required (NX-24): it is what makes a retried request the same request" };
    const id = wrIdFor(p.id, i.idempotencyKey);
    const existing = this.current(id);
    if (existing) return { ok: true, id, value: existing };

    let compensates: WriteRequestValue | null = null;
    if (i.compensates !== undefined) {
      if (typeof i.compensates !== "string" || !(compensates = this.current(i.compensates))) return { ok: false, status: 400, detail: "compensates must name an existing WriteRequest" };
      if (compensates.status !== "verified") return { ok: false, status: 409, detail: `only a verified request can be rolled back; ${i.compensates} is ${compensates.status}` };
      if (!compensates.reversible || !compensates.compensation) return { ok: false, status: 409, detail: `${i.compensates} was declared irreversible; no compensation exists (NX-35)` };
    }
    if (compensates) tier = compensates.tier;
    if (tier === "W2") {
      const why = this.d.w2Suspended?.();
      if (why) return { ok: false, status: 503, detail: `setpoint writes are suspended: ${why} (NX-53)` };
    }
    const targetId = compensates ? compensates.target.elementId : i.target?.elementId;
    if (typeof targetId !== "string") return { ok: false, status: 400, detail: "target.elementId is required" };
    if (p.kind === "agent" && this.d.agentCheck) {
      const used = p.grant ? this.ids().filter((x) => this.current(x)?.actor.grant?.id === p.grant!.id).length : 0;
      const why = this.d.agentCheck(p, tier, targetId, used);
      if (why) return { ok: false, status: this.d.agentWritesFrozen?.() ? 423 : 403, detail: why };
    }
    const field = compensates ? (compensates.target as { field?: string }).field ?? "value" : (typeof i.target?.field === "string" ? i.target.field : "value");
    const t = tier === "W2" ? this.d.resolveSetpoint?.(targetId, field) ?? null : this.d.resolveTarget(targetId);
    if (!t) {
      return { ok: false, status: 403, detail: tier === "W2"
        ? `${targetId}/${field} is not a governed setpoint on this nexus (a W2 type, a writable source and a declared envelope)`
        : `${targetId} is not a record in a system of record on this nexus; W1 writes target external records` };
    }
    const adapter = this.d.adapters.get(t.system)!;
    const proposed = compensates ? compensates.compensation!.proposed : i.proposed;
    if (!proposed || typeof proposed !== "object" || Array.isArray(proposed) || !Object.keys(proposed).length) return { ok: false, status: 400, detail: "proposed must be an object of the fields to set" };

    let prior: ExternalRecord | null = null;
    if (t.recordId !== null) {
      try { prior = await adapter.read(t.recordId); } catch (e) { return { ok: false, status: 503, detail: `system ${t.system} did not answer: ${(e as Error).message}` }; }
      if (!prior) return { ok: false, status: 404, detail: `${targetId} no longer exists in ${t.system}` };
    }
    const keys = Object.keys(proposed);
    // NX-28: the target is an authority limit. Validated now, so a bad change is refused before anyone is asked to approve it.
    const limitChange = t.system === AUTHORITY_SYSTEM ? parseLimit(prior?.fields) : null;
    if (t.system === AUTHORITY_SYSTEM) {
      const why = limitChange ? limitChangeProblem(prior!.fields, proposed) : "the current limit is not valid";
      if (why) return { ok: false, status: 422, detail: why };
    }
    if (tier === "W2") {
      if (keys.length !== 1 || keys[0] !== "value") return { ok: false, status: 400, detail: "a setpoint request proposes exactly { value }" };
      if (prior?.version === "not-good") return { ok: false, status: 409, detail: `${targetId}/${field} is not reading Good; a setpoint is never written blind` };
      // NX profile §7.4 step 4: outside the envelope is rejected, never clamped.
      const why = compensates ? null : this.d.envelopeCheck?.(t.recordId!, proposed.value, prior?.fields.value);
      if (why) return { ok: false, status: 422, detail: `outside the write envelope: ${why}` };
    }
    const reversible = compensates ? true : i.reversible !== false;
    let compensation: WriteRequestValue["compensation"] = null;
    if (reversible) {
      if (prior) compensation = { proposed: pick(prior.fields, keys) };
      else if (i.compensation?.proposed && typeof i.compensation.proposed === "object") compensation = { proposed: i.compensation.proposed };
      else return { ok: false, status: 400, detail: "creating a record needs compensation.proposed (how to undo it), or reversible: false" };
    }

    const matter = compensates ? compensates.matter : limitChange ? { site: limitChange.site, exceptionType: limitChange.exceptionType, exposureUsd: null, boundaryTriggers: [] } : {
      site: this.d.site,
      exceptionType: typeof i.exceptionType === "string" ? i.exceptionType : "",
      exposureUsd: typeof i.exposureUsd === "number" ? i.exposureUsd : null,
      boundaryTriggers: Array.isArray(i.boundaryTriggers) ? i.boundaryTriggers.filter((x: unknown): x is string => typeof x === "string") : [],
    };
    if (!matter.exceptionType) return { ok: false, status: 400, detail: "exceptionType is required: it selects the authority limit" };
    const found = limitChange ? { elementId: targetId, limit: limitChange } : this.findLimit(matter.exceptionType);
    const escalation = limitChange ? "escalates" : escalationOf(found?.limit ?? null, matter);
    let approverRoles = approverRolesFor(found?.limit ?? null, escalation, matter.exposureUsd);
    if ((limitChange || !reversible) && approverRoles) {
      // NX-35, and NX-28 for a change to the limit itself: one level above the owner.
      // A limit that names no such role cannot approve it.
      const above = found!.limit.aboveOwnerRole;
      approverRoles = above ? [above] : null;
    }
    const at = this.iso();
    const v: WriteRequestValue = {
      target: { elementId: targetId, ...t, ...(tier === "W2" ? { field } : {}) },
      tier,
      proposed,
      prior: { value: prior ? pick(prior.fields, keys) : null, version: prior?.version ?? null, timestamp: at },
      actor: p.grant
        ? { principal: p.id, kind: p.kind, onBehalfOf: p.grant.sponsor, grant: { id: p.grant.id, task: p.grant.task } }
        : { principal: p.id, kind: p.kind, onBehalfOf: typeof i.onBehalfOf === "string" ? i.onBehalfOf : null, grant: null },
      justification: { decisionRef: typeof i.justification?.decisionRef === "string" ? i.justification.decisionRef : (compensates?.justification.decisionRef ?? null), falsification: i.justification?.falsification ?? null },
      matter,
      authority: { limitRef: found?.elementId ?? null, escalation, approverRoles },
      reversible, compensation,
      compensates: compensates ? i.compensates : null,
      idempotencyKey: i.idempotencyKey,
      status: approverRoles ? "requested" : "rejected",
      statusDetail: approverRoles ? null : limitChange ? "the limit names no role above its owner, so it can be changed only in the site config (NX-28)" : (found ? "irreversible, and the limit names no role above its owner (NX-35)" : `no decision right: no authority limit for ${this.d.site} / ${matter.exceptionType}`),
      approvals: [], applied: null, verification: null,
      expiresAt: new Date(this.now() + (tier === "W2" ? (this.d.w2ExpiryMs ?? 15 * 60_000) : (this.d.expiryMs ?? 4 * 3600_000))).toISOString(),
    };
    await this.save(id, v, p.id, "requested");
    return { ok: true, id, value: v };
  }

  /** PUT on a WriteRequest: an approver writes { decision, comment }. */
  async decide(p: Principal, id: string, input: unknown): Promise<GovResult> {
    const cur = this.current(id);
    if (!cur) return { ok: false, status: 404, detail: `no WriteRequest ${id}` };
    const i = input as Record<string, unknown>;
    const decision = i?.decision;
    if (decision !== "approve" && decision !== "reject") return { ok: false, status: 400, detail: "an approver writes { decision: \"approve\" | \"reject\", comment? }; every other field is computed by the governor (NX-23)" };
    if (cur.status !== "requested") return { ok: false, status: 409, detail: `${id} is ${cur.status}; only a requested WriteRequest takes a decision` };
    if (Date.parse(cur.expiresAt) <= this.now()) { await this.expire(id); return { ok: false, status: 409, detail: `${id} expired at ${cur.expiresAt}` }; }
    if (p.kind === "agent") return { ok: false, status: 403, detail: "an agent may request but never approve a write (NX-21)" };
    if (cur.tier === "W2" && p.kind !== "person") return { ok: false, status: 403, detail: "a setpoint change is approved by a named person, every time" };
    if (p.id === cur.actor.principal) return { ok: false, status: 403, detail: "the requester cannot approve their own request" };
    // NXA-5: an agent's sponsor asked for this through the agent; approving it would be self-approval.
    if (cur.actor.grant && p.id === cur.actor.onBehalfOf) return { ok: false, status: 403, detail: `${p.id} sponsored the grant this request was raised under, so cannot approve it` };
    const role = (p.roles ?? []).find((r) => cur.authority.approverRoles?.includes(r));
    if (!role) return { ok: false, status: 403, detail: `approving ${id} needs one of: ${(cur.authority.approverRoles ?? []).join(", ") || "(nobody: no decision right)"}` };
    let signature: { signedHash: string; sig: string } | null = null;
    if (this.d.requireSignedApprovals || p.approvalKey) {
      // NXA-6: the approver signs the exact version they read, so a decision cannot be
      // replayed onto a request that changed after they looked at it.
      if (!p.approvalKey) return { ok: false, status: 403, detail: `${p.id} has no approval key registered; approvals on this nexus are signed` };
      const signedHash = this.d.records().current(id)!.hash;
      const sig = typeof i.signature === "string" ? i.signature : "";
      let good = false;
      try { good = !!sig && cryptoVerify(null, Buffer.from(approvalSigningString(id, decision, signedHash)), createPublicKey(p.approvalKey), Buffer.from(sig, "base64url")); } catch { good = false; }
      if (!good) return { ok: false, status: 403, detail: `signature missing or invalid; sign "${approvalSigningString(id, decision, signedHash)}" with your approval key (Ed25519, base64url)` };
      signature = { signedHash, sig };
    }

    const next: WriteRequestValue = {
      ...cur,
      approvals: [...cur.approvals, { principal: p.id, role, decision, comment: typeof i.comment === "string" ? i.comment : null, at: this.iso(), signature }],
      status: decision === "approve" ? "approved" : "rejected",
      statusDetail: decision === "approve" ? null : `rejected by ${p.id}`,
    };
    await this.save(id, next, p.id, `decision:${p.id}`);
    if (decision !== "approve") return { ok: true, id, value: next };
    try {
      return await this.apply(id);
    } catch (e) {
      // The approval stands; the system of record did not answer. The governor (this
      // node or the next one) applies it on its next round (`resume`).
      if ((e as Error).name === "NotLeaderError") throw e;
      return { ok: true, id, value: this.current(id)! };
    }
  }

  /** Approved → applied → verified, or rejected (stale) / rolled_back (read-back mismatch). */
  async apply(id: string): Promise<GovResult> {
    if (this.busy.has(id)) return { ok: true, id, value: this.current(id)! };
    this.busy.add(id);
    try {
      let cur = this.current(id)!;
      if (cur.status !== "approved") return { ok: true, id, value: cur };
      if (cur.tier === "W2") {
        // Held, not failed: it is applied when redundancy is back (resume), or expires.
        const why = this.d.w2Suspended?.();
        if (why) throw new Error(`setpoint writes are suspended: ${why}`);
      }
      if (cur.actor.kind === "agent") {
        // Held, like a suspended setpoint: applied when the freeze is released, or never.
        const fz = this.d.agentWritesFrozen?.();
        if (fz) throw new Error(fz);
      }
      const adapter = this.d.adapters.get(cur.target.system)!;
      // Freshness (§7.4 step 5): the record must still be what the request was raised against.
      const now = cur.target.recordId ? await adapter.read(cur.target.recordId) : null;
      if (cur.target.recordId) {
        if (!now) return this.finish(id, { ...cur, status: "rejected", statusDetail: "the record no longer exists" }, "stale");
        if (now.version !== cur.prior.version) {
          // Already applied by a governor that died before recording it?
          if (sameFields(cur.proposed, now.fields)) return this.recordApplied(id, cur, now);
          return this.finish(id, { ...cur, status: "rejected", statusDetail: `stale: ${cur.target.elementId} is at version ${now.version}, the request was raised at ${cur.prior.version}` }, "stale");
        }
      }
      const r = await adapter.apply(cur.target.recordId, cur.proposed, cur.prior.version);
      if (!r.ok) {
        if (r.stale) {
          const again = cur.target.recordId ? await adapter.read(cur.target.recordId) : null;
          if (again && sameFields(cur.proposed, again.fields)) return this.recordApplied(id, cur, again);
          return this.finish(id, { ...cur, status: "rejected", statusDetail: `stale: ${cur.target.system} refused the conditional write (${r.status})` }, "stale");
        }
        // A 5xx is the system failing, not deciding: the approval stands and the
        // governor retries on its next round. Only a 4xx is a refusal.
        if (r.status >= 500) throw new Error(`${cur.target.system} failed (${r.status}); will retry`);
        return this.finish(id, { ...cur, status: "rejected", statusDetail: `${cur.target.system} refused the write: ${r.status} ${r.detail}` }, "refused");
      }
      cur = (await this.recordApplied(id, cur, r.record, false)).value!;
      return this.verify(id);
    } finally {
      this.busy.delete(id);
    }
  }

  private async recordApplied(id: string, cur: WriteRequestValue, rec: ExternalRecord, thenVerify = true): Promise<GovResult & { value?: WriteRequestValue }> {
    const next: WriteRequestValue = {
      ...cur, status: "applied", applied: { at: this.iso(), ack: rec.version },
      target: cur.target.recordId ? cur.target : { ...cur.target, recordId: rec.id },
    };
    await this.save(id, next, SYSTEM, "applied");
    if (thenVerify) return this.verify(id);
    return { ok: true, id, value: next };
  }

  /** NX-36: read back. NX-34: a mismatch is compensated at once. */
  async verify(id: string): Promise<GovResult> {
    const cur = this.current(id)!;
    const adapter = this.d.adapters.get(cur.target.system)!;
    const deadline = this.now() + (this.d.verifyWindowMs ?? 10_000);
    let back: ExternalRecord | null = null;
    for (;;) {
      back = await adapter.read(cur.target.recordId!).catch(() => null);
      if (back && sameFields(cur.proposed, back.fields)) break;
      if (this.now() >= deadline) break;
      await new Promise((r) => setTimeout(r, 250));
    }
    const match = !!back && sameFields(cur.proposed, back.fields);
    const verification = { readBack: back ? pick(back.fields, Object.keys(cur.proposed)) : null, match, at: this.iso() };
    if (match) return this.finish(id, { ...cur, status: "verified", verification }, "verified");
    // The approved state never existed: put back what was there, without asking.
    let detail = "read-back did not match the approved value";
    if (cur.compensation && back) {
      const c = await adapter.apply(cur.target.recordId, cur.compensation.proposed, back.version, { compensating: true });
      detail += c.ok ? "; compensation applied" : `; compensation FAILED (${c.status} ${c.detail}) — needs a person`;
    } else detail += "; no compensation exists — needs a person";
    return this.finish(id, { ...cur, status: "rolled_back", statusDetail: detail, verification }, "rolled_back");
  }

  private async finish(id: string, v: WriteRequestValue, key: string): Promise<GovResult> {
    await this.save(id, v, SYSTEM, key);
    return { ok: true, id, value: v };
  }

  async expire(id: string) {
    const cur = this.current(id);
    if (cur?.status === "requested") await this.finish(id, { ...cur, status: "expired", statusDetail: "not approved before it expired; nothing was applied" }, "expired");
  }

  /** On becoming governor, and periodically: expire stale requests, finish interrupted ones. */
  async resume() {
    for (const id of this.ids()) {
      const v = this.current(id);
      if (!v) continue;
      try {
        if (v.status === "requested" && Date.parse(v.expiresAt) <= this.now()) await this.expire(id);
        else if (v.status === "approved") await this.apply(id);
        else if (v.status === "applied") await this.verify(id);
      } catch { /* the system of record is down; try again next round */ }
    }
  }
}
