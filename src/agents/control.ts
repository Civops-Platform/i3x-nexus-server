// Agents as a managed population (Agent extension §2–3).
//
// An agent never acts on a standing credential of its own. A named person issues it
// a grant: one task, the areas of the plant it covers, what it may do there, how many
// write requests it may raise, and when it stops working. The agent presents the
// grant's token; every write request it raises records the grant and the person who
// sponsored it, so the ledger reads "agent X, for person Y, on task Z" (NXA-2).
//
// Three controls sit above the grants:
//   freeze    one switch stops every agent write on the site, new requests and the
//             apply of agent requests already approved; people and systems carry on.
//             Any person with agents:freeze can pull it; only agents:admin releases it.
//   suspend   one agent is shut out entirely, reads included.
//   revoke    one grant stops working; the sponsor or an admin can do it.
//
// Grants and the control state are ledger records (`nexus:agent-grant:*`,
// `nexus:agent-control`), so they are hash-chained like every other write, replicated
// to the standby in a pair, and their history is the audit trail of who let which
// agent do what. They are not i3X elements: an agent cannot read another's grant.

import { createHash, randomBytes, timingSafeEqual } from "node:crypto";
import type { Principal, PrincipalDef } from "../core/auth";
import type { RecordStore } from "../core/records";

export const GRANT_PREFIX = "nexus:agent-grant:";
export const CONTROL_ID = "nexus:agent-control";
export const GRANT_TOKEN_PREFIX = "nxg_";

export type GrantAction = "read" | "media:read" | "request:W1" | "request:W2";
const ACTIONS: GrantAction[] = ["read", "media:read", "request:W1", "request:W2"];
const MAX_TTL_MIN = 24 * 60;

export interface Grant {
  id: string;
  agent: string;
  sponsor: string;
  task: string;
  /** Element ids; the grant covers each and everything below it. Empty = nothing. */
  areas: string[];
  actions: GrantAction[];
  maxRequests: number;
  issuedAt: string;
  expiresAt: string;
  tokenSha256: string;
  status: "active" | "revoked";
  revoked: { by: string; at: string; reason: string | null } | null;
}

/** What a request authenticated by a grant carries with it. */
export interface GrantContext { id: string; sponsor: string; task: string; areas: string[]; actions: GrantAction[]; maxRequests: number; expiresAt: string }

export interface ControlState {
  frozen: boolean;
  frozenDetail: { by: string; at: string; reason: string } | null;
  suspended: string[];
}

export interface Flag { at: string; agent: string; grant: string | null; what: string }

export type CtlResult<T> = { ok: true; value: T } | { ok: false; status: number; detail: string };

export interface AgentControlDeps {
  records: () => RecordStore;
  /** Every configured principal, for agent and sponsor lookups. */
  principals: PrincipalDef[];
  /** Parent of an element in the address space, or null at the top. */
  parentOf: (elementId: string) => string | null | undefined;
  /** Agent extension: an agent with no grant may only read /info. */
  requireGrant: boolean;
  now?: () => number;
}

const sha256 = (s: string) => createHash("sha256").update(s).digest("hex");

export class AgentControl {
  private flags: Flag[] = [];
  constructor(private d: AgentControlDeps) {}

  private now() { return this.d.now?.() ?? Date.now(); }
  private iso() { return new Date(this.now()).toISOString(); }
  get requireGrant() { return this.d.requireGrant; }

  state(): ControlState {
    return (this.d.records().current(CONTROL_ID)?.value as ControlState) ?? { frozen: false, frozenDetail: null, suspended: [] };
  }
  frozen(): string | null {
    const s = this.state();
    return s.frozen ? `agent writes are frozen site-wide by ${s.frozenDetail?.by} at ${s.frozenDetail?.at}: ${s.frozenDetail?.reason}` : null;
  }
  grant(id: string): Grant | null { return (this.d.records().current(GRANT_PREFIX + id)?.value as Grant) ?? null; }
  grants(): Grant[] {
    return this.d.records().ids().filter((i) => i.startsWith(GRANT_PREFIX)).map((i) => this.d.records().current(i)!.value as Grant);
  }

  /** Something an agent tried and was refused, kept for the registry (in memory, newest 500). */
  flag(agent: string, grant: string | null, what: string) {
    this.flags.push({ at: this.iso(), agent, grant, what });
    if (this.flags.length > 500) this.flags.splice(0, this.flags.length - 500);
  }
  recentFlags(agent?: string) { return this.flags.filter((f) => !agent || f.agent === agent); }

  // ── authentication ────────────────────────────────────────────────────────

  /** A grant token resolves to its agent, with the grant attached. Expired or revoked: null. */
  resolve(token: string): Principal | null {
    if (!token.startsWith(GRANT_TOKEN_PREFIX)) return null;
    const [id] = token.slice(GRANT_TOKEN_PREFIX.length).split(".");
    const g = id ? this.grant(id) : null;
    if (!g || g.status !== "active" || Date.parse(g.expiresAt) <= this.now()) return null;
    const a = Buffer.from(g.tokenSha256, "hex"), b = Buffer.from(sha256(token), "hex");
    if (a.length !== b.length || !timingSafeEqual(a, b)) return null;
    const def = this.d.principals.find((p) => p.id === g.agent && p.kind === "agent");
    if (!def) return null;
    const scopes = ["read", ...(g.actions.includes("media:read") ? ["media:read"] : [])];
    return {
      id: def.id, kind: "agent", scopes, owns: [], roles: [],
      grant: { id: g.id, sponsor: g.sponsor, task: g.task, areas: g.areas, actions: g.actions, maxRequests: g.maxRequests, expiresAt: g.expiresAt },
    };
  }

  /** Gate on every authenticated request. Null = go on. */
  admit(p: Principal): { status: number; detail: string } | null {
    if (p.kind !== "agent") return null;
    if (this.state().suspended.includes(p.id)) { this.flag(p.id, p.grant?.id ?? null, "suspended agent tried to connect"); return { status: 403, detail: `${p.id} is suspended` }; }
    if (this.d.requireGrant && !p.grant) {
      this.flag(p.id, null, "connected without a grant");
      return { status: 403, detail: "an agent acts under a grant from a named person (NXA-1); ask your sponsor for one" };
    }
    return null;
  }

  // ── scope checks ──────────────────────────────────────────────────────────

  /** Is this element (or an attribute of it) inside the grant's areas? */
  inArea(p: Principal, elementId: string): boolean {
    if (!p.grant) return true;
    if (elementId.startsWith("nexus:intake:") || elementId === "nexus:requests") return true;
    let id: string | null | undefined = elementId;
    // An attribute id is object/attribute; the object decides.
    if (this.d.parentOf(id) === undefined && id.includes("/")) id = id.slice(0, id.lastIndexOf("/"));
    for (let hops = 0; id && hops < 64; hops++) {
      if (p.grant.areas.includes(id)) return true;
      id = this.d.parentOf(id);
    }
    return false;
  }

  /** Before the governor accepts a write request from this principal. Null = allowed. */
  checkRequest(p: Principal, tier: "W1" | "W2", targetId: string, used: number): string | null {
    if (p.kind !== "agent") return null;
    const fz = this.frozen();
    if (fz) { this.flag(p.id, p.grant?.id ?? null, `request refused while frozen: ${targetId}`); return fz; }
    if (!p.grant) return this.d.requireGrant ? "an agent acts under a grant from a named person (NXA-1)" : null;
    const g = p.grant;
    const why = !g.actions.includes(`request:${tier}`) ? `grant ${g.id} does not allow ${tier} requests`
      : !this.inArea(p, targetId) ? `${targetId} is outside grant ${g.id}'s areas (${g.areas.join(", ")})`
      : used >= g.maxRequests ? `grant ${g.id} has used all ${g.maxRequests} of its requests`
      : null;
    if (why) this.flag(p.id, g.id, why);
    return why;
  }

  // ── administration ────────────────────────────────────────────────────────

  private canAdmin(p: Principal) { return p.kind === "person" && p.scopes.includes("agents:admin"); }

  async issue(p: Principal, input: unknown): Promise<CtlResult<{ grant: Omit<Grant, "tokenSha256">; token: string }>> {
    if (p.kind !== "person" || !p.scopes.includes("agents:grant")) return { ok: false, status: 403, detail: "only a person with the agents:grant scope issues grants, and is the grant's sponsor" };
    const i = (input ?? {}) as Record<string, unknown>;
    const agent = this.d.principals.find((x) => x.id === i.agent);
    if (!agent || agent.kind !== "agent") return { ok: false, status: 400, detail: "agent must name a registered agent principal" };
    if (this.state().suspended.includes(agent.id)) return { ok: false, status: 409, detail: `${agent.id} is suspended` };
    if (typeof i.task !== "string" || !i.task.trim()) return { ok: false, status: 400, detail: "task is required: the ledger records what the agent was asked to do" };
    const areas = Array.isArray(i.areas) ? i.areas.filter((x): x is string => typeof x === "string") : [];
    if (!areas.length) return { ok: false, status: 400, detail: "areas is required: the element ids the agent may work under" };
    const unknown = areas.filter((a) => this.d.parentOf(a) === undefined);
    if (unknown.length) return { ok: false, status: 400, detail: `not elements on this nexus: ${unknown.join(", ")}` };
    const actions = Array.isArray(i.actions) ? i.actions.filter((x): x is GrantAction => ACTIONS.includes(x as GrantAction)) : ["read" as const];
    if (!actions.includes("read")) actions.unshift("read");
    const maxRequests = Number.isInteger(i.maxRequests) && (i.maxRequests as number) >= 0 ? (i.maxRequests as number) : 0;
    const ttl = Number.isFinite(i.ttlMinutes) ? Math.min(Math.max(1, i.ttlMinutes as number), MAX_TTL_MIN) : 60;
    const id = randomBytes(9).toString("base64url");
    const token = `${GRANT_TOKEN_PREFIX}${id}.${randomBytes(24).toString("base64url")}`;
    const g: Grant = {
      id, agent: agent.id, sponsor: p.id, task: i.task.trim(), areas, actions, maxRequests,
      issuedAt: this.iso(), expiresAt: new Date(this.now() + ttl * 60_000).toISOString(),
      tokenSha256: sha256(token), status: "active", revoked: null,
    };
    await this.d.records().write(GRANT_PREFIX + id, g, p.id, undefined, `${GRANT_PREFIX}${id}:issued`);
    const { tokenSha256: _t, ...pub } = g;
    return { ok: true, value: { grant: pub, token } };
  }

  async revoke(p: Principal, id: string, input: unknown): Promise<CtlResult<Grant>> {
    const g = this.grant(id);
    if (!g) return { ok: false, status: 404, detail: `no grant ${id}` };
    if (!(p.kind === "person" && (p.id === g.sponsor || this.canAdmin(p)))) return { ok: false, status: 403, detail: "the grant's sponsor or an agents:admin revokes it" };
    if (g.status === "revoked") return { ok: true, value: g };
    const reason = typeof (input as Record<string, unknown>)?.reason === "string" ? String((input as Record<string, unknown>).reason) : null;
    const next: Grant = { ...g, status: "revoked", revoked: { by: p.id, at: this.iso(), reason } };
    await this.d.records().write(GRANT_PREFIX + id, next, p.id, undefined, `${GRANT_PREFIX}${id}:revoked`);
    return { ok: true, value: next };
  }

  async setFrozen(p: Principal, frozen: boolean, input: unknown): Promise<CtlResult<ControlState>> {
    if (p.kind !== "person") return { ok: false, status: 403, detail: "a person pulls or releases the freeze" };
    if (frozen && !(p.scopes.includes("agents:freeze") || this.canAdmin(p))) return { ok: false, status: 403, detail: "freezing agents needs agents:freeze" };
    if (!frozen && !this.canAdmin(p)) return { ok: false, status: 403, detail: "releasing the freeze needs agents:admin" };
    const reason = String((input as Record<string, unknown>)?.reason ?? "").trim();
    if (!reason) return { ok: false, status: 400, detail: "reason is required" };
    const s = this.state();
    if (s.frozen === frozen) return { ok: true, value: s };
    const next: ControlState = { ...s, frozen, frozenDetail: frozen ? { by: p.id, at: this.iso(), reason } : null };
    await this.d.records().write(CONTROL_ID, next, p.id, undefined, `${CONTROL_ID}:${frozen ? "freeze" : "release"}:${this.now()}`);
    return { ok: true, value: next };
  }

  async setSuspended(p: Principal, agent: string, suspended: boolean): Promise<CtlResult<ControlState>> {
    if (!this.canAdmin(p)) return { ok: false, status: 403, detail: "suspending an agent needs agents:admin" };
    if (!this.d.principals.some((x) => x.id === agent && x.kind === "agent")) return { ok: false, status: 404, detail: `no agent ${agent}` };
    const s = this.state();
    const set = new Set(s.suspended);
    if (suspended) set.add(agent); else set.delete(agent);
    const next: ControlState = { ...s, suspended: [...set].sort() };
    await this.d.records().write(CONTROL_ID, next, p.id, undefined, `${CONTROL_ID}:${suspended ? "suspend" : "resume"}:${agent}:${this.now()}`);
    return { ok: true, value: next };
  }

  /**
   * The registry: every agent, its grants, how its requests have fared and what it was
   * refused. `requests` are the WriteRequest values the governor holds.
   */
  registry(p: Principal, requests: Array<{ actor: { principal: string; grant?: { id: string } | null }; status: string }>) {
    const admin = this.canAdmin(p);
    if (!admin && !(p.kind === "person" && p.scopes.includes("agents:grant"))) return null;
    const s = this.state();
    const now = this.now();
    const agents = this.d.principals.filter((x) => x.kind === "agent").map((a) => {
      const mine = requests.filter((r) => r.actor.principal === a.id);
      const byStatus: Record<string, number> = {};
      for (const r of mine) byStatus[r.status] = (byStatus[r.status] ?? 0) + 1;
      const decided = (byStatus.verified ?? 0) + (byStatus.rejected ?? 0) + (byStatus.rolled_back ?? 0) + (byStatus.applied ?? 0);
      const grants = this.grants().filter((g) => g.agent === a.id && (admin || g.sponsor === p.id)).map(({ tokenSha256: _t, ...g }) => ({
        ...g,
        live: g.status === "active" && Date.parse(g.expiresAt) > now,
        used: requests.filter((r) => r.actor.grant?.id === g.id).length,
      }));
      if (!admin && !grants.length) return null;
      return {
        id: a.id,
        suspended: s.suspended.includes(a.id),
        grants,
        record: { requests: mine.length, byStatus, verifiedShare: decided ? (byStatus.verified ?? 0) / decided : null },
        flags: this.recentFlags(a.id).slice(-20),
      };
    }).filter((x) => x !== null);
    return { frozen: s.frozen, frozenDetail: s.frozenDetail, requireGrant: this.d.requireGrant, agents };
  }
}
