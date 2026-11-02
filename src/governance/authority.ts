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
