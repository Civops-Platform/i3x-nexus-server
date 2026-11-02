// Principals, scopes and rate limits (spec §9, FR1/FR2/FR7).
//
// One principal per person, agent or service, never a shared token for anything
// that writes. Phase 1 authenticates by bearer token (what i3X clients carry
// today); tokens are configured as SHA-256 hashes so the principals file is not
// itself a credential. OAuth 2 client credentials and mTLS are phase 3.
//
// Scopes:
//   read            every read, subscription and stream
//   media:read      fetch media bytes (video and audio can record people, §9 FR4)
//   media:upload    request an upload on the media intake
//   owns            namespaces this principal may publish into (tier W0)

import { createHash, createHmac, timingSafeEqual } from "node:crypto";
import type { JwtVerifier } from "./oauth";

export interface Principal {
  id: string;
  kind: "person" | "agent" | "service";
  scopes: string[];
  owns: string[];
  /** Roles this principal holds for approvals (W1/W2). Agents hold none that approve. */
  roles?: string[];
}

/**
 * How a principal proves who it is. At least one of these; each maps to exactly
 * this principal, and roles/scopes/owned namespaces always come from here.
 */
export interface PrincipalDef extends Principal {
  /** SHA-256 of an opaque bearer token. */
  tokenSha256?: string;
  /** The `sub` of OAuth 2 access tokens from the configured identity provider. */
  oauthSubject?: string;
  /** Client-certificate subject CN (mTLS), issued by the configured CA. */
  certSubject?: string;
}

export const sha256 = (s: string) => createHash("sha256").update(s).digest("hex");

export interface AuthRequest {
  authorization?: string;
  /** The verified client certificate's subject CN, when the TLS layer authorised one. */
  certSubject?: string | null;
  /** A node-to-node identity assertion on a forwarded write. */
  forwardedPrincipal?: string;
}

export class Authenticator {
  private byHash: Array<{ hash: Buffer; p: Principal }>;
  private bySub = new Map<string, Principal>();
  private byCert = new Map<string, Principal>();
  private byId = new Map<string, Principal>();

  constructor(defs: PrincipalDef[], private jwt: JwtVerifier | null = null, private clusterSecret: string | null = null) {
    const strip = ({ tokenSha256: _t, oauthSubject: _o, certSubject: _c, ...p }: PrincipalDef): Principal => p;
    this.byHash = defs.filter((d) => d.tokenSha256).map((d) => ({ hash: Buffer.from(d.tokenSha256!, "hex"), p: strip(d) }));
    for (const d of defs) {
      if (d.oauthSubject) this.bySub.set(d.oauthSubject, strip(d));
      if (d.certSubject) this.byCert.set(d.certSubject, strip(d));
      this.byId.set(d.id, strip(d));
    }
  }

  /** Opaque bearer tokens only (kept for callers that have nothing else). */
  fromHeader(h: string | undefined): Principal | null {
    const m = /^Bearer\s+(.+)$/i.exec(h ?? "");
    if (!m) return null;
    const got = createHash("sha256").update(m[1].trim()).digest();
    for (const { hash, p } of this.byHash) if (hash.length === got.length && timingSafeEqual(hash, got)) return p;
    return null;
  }

  /** mTLS first, then a forwarded-write assertion, then a JWT, then an opaque token. */
  async fromRequest(r: AuthRequest): Promise<Principal | null> {
    if (r.certSubject) { const p = this.byCert.get(r.certSubject); if (p) return p; }
    if (r.forwardedPrincipal && this.clusterSecret) {
      const p = this.verifyForwarded(r.forwardedPrincipal);
      if (p) return p;
    }
    const m = /^Bearer\s+(.+)$/i.exec(r.authorization ?? "");
    if (!m) return null;
    const tok = m[1].trim();
    if (this.jwt && tok.split(".").length === 3) {
      const v = await this.jwt.verify(tok);
      return v.ok ? this.bySub.get(v.subject) ?? null : null;
    }
    return this.fromHeader(r.authorization);
  }

  /**
   * A standby forwarding a write to the governor vouches for the caller it already
   * authenticated (needed for mTLS callers, whose certificate does not travel). Signed
   * with the cluster secret, valid for 30 s.
   */
  assertForwarded(principalId: string, now = Date.now()) {
    const exp = Math.floor(now / 1000) + 30;
    const sig = createHmac("sha256", this.clusterSecret ?? "").update(`fwd|${principalId}|${exp}`).digest("base64url");
    return `${Buffer.from(principalId).toString("base64url")}.${exp}.${sig}`;
  }

  private verifyForwarded(v: string, now = Date.now()): Principal | null {
    const [idb, exp, sig] = v.split(".");
    if (!idb || !exp || !sig || Number(exp) * 1000 < now) return null;
    const id = Buffer.from(idb, "base64url").toString();
    const want = createHmac("sha256", this.clusterSecret!).update(`fwd|${id}|${exp}`).digest();
    const got = Buffer.from(sig, "base64url");
    if (got.length !== want.length || !timingSafeEqual(got, want)) return null;
    return this.byId.get(id) ?? null;
  }
}

export const can = (p: Principal | null, scope: string) => !!p && p.scopes.includes(scope);

/**
 * Namespace ownership, the W0 test (spec §6.4 step 2). Exact membership: a prefix
 * match would let `https://example.com.evil` publish as `https://example.com`.
 * Fails closed on a missing namespace.
 */
export const owns = (p: Principal | null, namespaceUri: string | undefined) =>
  !!p && typeof namespaceUri === "string" && namespaceUri.length > 0 && p.owns.includes(namespaceUri);

/** Token bucket per principal: `rate` per second sustained, `burst` at once. */
export class RateLimiter {
  private buckets = new Map<string, { tokens: number; at: number }>();
  constructor(private rate = 25, private burst = 120) {}
  take(key: string, now = Date.now()): { ok: true } | { ok: false; retryAfterS: number } {
    const b = this.buckets.get(key) ?? { tokens: this.burst, at: now };
    b.tokens = Math.min(this.burst, b.tokens + ((now - b.at) / 1000) * this.rate);
    b.at = now;
    this.buckets.set(key, b);
    if (b.tokens >= 1) { b.tokens -= 1; return { ok: true }; }
    return { ok: false, retryAfterS: Math.ceil((1 - b.tokens) / this.rate) };
  }
}
