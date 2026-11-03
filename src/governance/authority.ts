// Authority limits and the escalation rule (profile §7.4).
//
// A limit is the rule for one site and one exception type: who owns the decision,
// up to what exposure it is delegated, and the materiality threshold above which it
// escalates whatever else is true. It is data, not code: a limit is a record in an
// authority namespace, published by whatever system holds decision rights (an
// operations platform, a governance register) as an ordinary W0 write, or declared
// in the site config.
//
// The rule, exactly as the profile states it:
//   escalates          exposure ≥ materiality threshold, or any boundary trigger
//   no_decision_right  no limit exists for this site and exception type
//   exposure_unknown   a limit exists but no exposure is stated
//   stays_local        everything else
// The delegated limit decides WHO approves. It is not the escalation test.

export interface AuthorityLimit {
  site: string;
  exceptionType: string;
  /** The role that owns the decision; approves anything that escalates. */
  ownerRole: string;
  /** Roles that may approve below the delegated limit. */
  approverRoles: string[];
  delegatedLimitUsd: number;
  materialityThresholdUsd: number;
  /** Conditions that escalate regardless of exposure, e.g. "safety", "customer-quality". */
  boundaryTriggers: string[];
  /** The role one level above the owner: approves irreversible requests (NX-35). */
  aboveOwnerRole?: string;
}

export type Escalation = "escalates" | "no_decision_right" | "exposure_unknown" | "stays_local";

export interface MatterFacts { exposureUsd?: number | null; boundaryTriggers?: string[] }

export function escalationOf(limit: AuthorityLimit | null, m: MatterFacts): Escalation {
  const triggered = (m.boundaryTriggers ?? []).length > 0;
  if (!limit) return triggered ? "escalates" : "no_decision_right";
  if (triggered) return "escalates";
  if (m.exposureUsd === null || m.exposureUsd === undefined) return "exposure_unknown";
  if (m.exposureUsd >= limit.materialityThresholdUsd) return "escalates";
  return "stays_local";
}

/**
 * The roles that may approve, or null when nobody may (no decision right). Above
 * the delegated limit, or when it escalates or the exposure is unknown, only the
 * owner role approves. Reversibility is handled by the caller (NX-35 raises it a level).
 */
export function approverRolesFor(limit: AuthorityLimit | null, esc: Escalation, exposureUsd?: number | null): string[] | null {
  if (!limit || esc === "no_decision_right") return null;
  if (esc === "escalates" || esc === "exposure_unknown") return [limit.ownerRole];
  if ((exposureUsd ?? 0) > limit.delegatedLimitUsd) return [limit.ownerRole];
  return [...new Set([...limit.approverRoles, limit.ownerRole])];
}

export function parseLimit(v: unknown): AuthorityLimit | null {
  if (!v || typeof v !== "object") return null;
  const o = v as Record<string, unknown>;
  const num = (x: unknown) => (typeof x === "number" && Number.isFinite(x) ? x : null);
  if (typeof o.site !== "string" || typeof o.exceptionType !== "string" || typeof o.ownerRole !== "string") return null;
  const d = num(o.delegatedLimitUsd), m = num(o.materialityThresholdUsd);
  if (d === null || m === null) return null;
  return {
    site: o.site, exceptionType: o.exceptionType, ownerRole: o.ownerRole,
    approverRoles: Array.isArray(o.approverRoles) ? o.approverRoles.filter((x): x is string => typeof x === "string") : [],
    delegatedLimitUsd: d, materialityThresholdUsd: m,
    boundaryTriggers: Array.isArray(o.boundaryTriggers) ? o.boundaryTriggers.filter((x): x is string => typeof x === "string") : [],
    ...(typeof o.aboveOwnerRole === "string" ? { aboveOwnerRole: o.aboveOwnerRole } : {}),
  };
}

// ── Changing a limit (NX-28) ─────────────────────────────────────────────
//
// A limit constrains the people it names, so they must not be able to widen it. A
// change to a limit is a governed request like any other W1 write, with its target
// the limit itself and its approver fixed one level above the limit's owner
// (aboveOwnerRole, as NX-35 does for irreversible writes). A plain W0 write to a
// limit is refused. A limit that names no role above its owner can be changed only
// in the site config.

import type { RecordStore } from "../core/records";
import type { ApplyResult, ExternalRecord, SystemAdapter } from "./adapters";

export const AUTHORITY_SYSTEM = "nexus:authority";
export const LIMIT_TYPE = "AuthorityLimit";

/** Why a proposed change to `current` is not a valid limit for the same site and exception type, or null. */
export function limitChangeProblem(current: Record<string, unknown>, proposed: Record<string, unknown>): string | null {
  const before = parseLimit(current);
  const after = parseLimit({ ...current, ...proposed });
  if (!before || !after) return "the result would not be a valid AuthorityLimit (site, exceptionType, ownerRole and both thresholds are required)";
  if (after.site !== before.site || after.exceptionType !== before.exceptionType) return "a limit's site and exceptionType are its identity; publish a new limit in the site config instead";
  return null;
}

/** The nexus's own AuthorityLimit records, presented to the governor as a system of record. */
export class LimitAdapter implements SystemAdapter {
  readonly id = AUTHORITY_SYSTEM;
  constructor(private records: () => RecordStore, private isLimit: (elementId: string) => boolean) {}

  async list(): Promise<ExternalRecord[]> { return []; }

  async read(recordId: string): Promise<ExternalRecord | null> {
    const v = this.isLimit(recordId) ? this.records().current(recordId) : undefined;
    return v ? { id: recordId, version: String(v.version), fields: { ...(v.value as Record<string, unknown>) } } : null;
  }

  async apply(recordId: string | null, fields: Record<string, unknown>, ifVersion: string | null): Promise<ApplyResult> {
    const cur = recordId ? await this.read(recordId) : null;
    if (!cur) return { ok: false, stale: false, status: 404, detail: "limits are created in the site config, not through a write request" };
    if (ifVersion !== null && cur.version !== ifVersion) return { ok: false, stale: true, status: 409, detail: `limit is at version ${cur.version}` };
    const why = limitChangeProblem(cur.fields, fields);
    if (why) return { ok: false, stale: false, status: 422, detail: why };
    const v = await this.records().write(recordId!, { ...cur.fields, ...fields }, "nexus:governor", undefined, `${recordId}:limit-change:${cur.version}`);
    return { ok: true, record: { id: recordId!, version: String(v.version), fields: { ...(v.value as Record<string, unknown>) } } };
  }
}
