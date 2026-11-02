// OAuth 2 access tokens (JWT) from the site's identity provider (profile §10, FR1).
//
// The identity provider says WHO the caller is; the nexus decides WHAT they may do.
// A token is accepted only if:
//   - its signature verifies against a key in the provider's JWKS (fetched, cached,
//     refetched once on an unknown key id), with an algorithm on the allow-list
//     (RS256, PS256, ES256; never "none", never HMAC with a public key);
//   - iss equals the configured issuer, aud includes the configured audience;
//   - exp is in the future and nbf/iat are not, within 60 s of clock skew.
// The subject then has to be a principal this nexus registers (`oauthSubject` in the
// principals file). Roles, scopes and owned namespaces come from that registration,
// never from claims, so a token minted with "roles": ["plant-manager"] grants nothing
// the nexus did not grant already.

import { createPublicKey, verify as cryptoVerify, type KeyObject } from "node:crypto";

type Jwk = Record<string, unknown> & { kty?: string; kid?: string; use?: string };

export interface OAuthConfig { issuer: string; jwksUrl: string; audience: string; subjectClaim?: string }

const ALGS: Record<string, { hash: string; kty: string; pss?: boolean; ecdsa?: boolean }> = {
  RS256: { hash: "sha256", kty: "RSA" },
  PS256: { hash: "sha256", kty: "RSA", pss: true },
  ES256: { hash: "sha256", kty: "EC", ecdsa: true },
};
const SKEW_S = 60;

const b64url = (s: string) => Buffer.from(s, "base64url");

export type JwtResult = { ok: true; subject: string; claims: Record<string, unknown> } | { ok: false; reason: string };

export class JwtVerifier {
  private keys = new Map<string, { key: KeyObject; kty: string }>();
  private fetchedAt = 0;
  private inflight: Promise<void> | null = null;

  constructor(private cfg: OAuthConfig, private now: () => number = Date.now) {}

  private async loadKeys(force = false) {
    if (!force && this.now() - this.fetchedAt < 10 * 60_000 && this.keys.size) return;
    this.inflight ??= (async () => {
      try {
        const r = await fetch(this.cfg.jwksUrl, { signal: AbortSignal.timeout(5_000) });
        if (!r.ok) throw new Error(`JWKS HTTP ${r.status}`);
        const { keys } = (await r.json()) as { keys: Jwk[] };
        const next = new Map<string, { key: KeyObject; kty: string }>();
        for (const k of keys ?? []) {
          if (!k.kid || (k.use && k.use !== "sig")) continue;
          try { next.set(k.kid, { key: createPublicKey({ key: k as never, format: "jwk" }), kty: String(k.kty) }); } catch { /* skip a key we cannot use */ }
        }
        this.keys = next;
        this.fetchedAt = this.now();
      } finally { this.inflight = null; }
    })();
    await this.inflight;
  }

  async verify(token: string): Promise<JwtResult> {
    const parts = token.split(".");
    if (parts.length !== 3) return { ok: false, reason: "not a JWT" };
    let header: Record<string, unknown>, claims: Record<string, unknown>;
    try { header = JSON.parse(b64url(parts[0]).toString()); claims = JSON.parse(b64url(parts[1]).toString()); } catch { return { ok: false, reason: "malformed JWT" }; }
    const alg = ALGS[String(header.alg)];
    if (!alg) return { ok: false, reason: `algorithm ${String(header.alg)} is not accepted` };
    const kid = String(header.kid ?? "");
    try { await this.loadKeys(); } catch (e) { return { ok: false, reason: `cannot fetch the identity provider's keys: ${(e as Error).message}` }; }
    if (!this.keys.has(kid)) { try { await this.loadKeys(true); } catch { /* fall through */ } }
    const k = this.keys.get(kid);
    if (!k) return { ok: false, reason: "signing key not found" };
    if (k.kty !== alg.kty) return { ok: false, reason: "key type does not match the algorithm" };
    const data = Buffer.from(`${parts[0]}.${parts[1]}`);
    const sig = b64url(parts[2]);
    const good = alg.ecdsa
      ? cryptoVerify(alg.hash, data, { key: k.key, dsaEncoding: "ieee-p1363" }, sig)
      : cryptoVerify(alg.hash, data, alg.pss ? { key: k.key, padding: 6 /* RSA_PKCS1_PSS_PADDING */, saltLength: 32 } : k.key, sig);
    if (!good) return { ok: false, reason: "signature does not verify" };
    const t = Math.floor(this.now() / 1000);
    if (claims.iss !== this.cfg.issuer) return { ok: false, reason: "wrong issuer" };
    const aud = Array.isArray(claims.aud) ? claims.aud : [claims.aud];
    if (!aud.includes(this.cfg.audience)) return { ok: false, reason: "wrong audience" };
    if (typeof claims.exp !== "number" || claims.exp + SKEW_S < t) return { ok: false, reason: "expired" };
    if (typeof claims.nbf === "number" && claims.nbf - SKEW_S > t) return { ok: false, reason: "not yet valid" };
    const sub = claims[this.cfg.subjectClaim ?? "sub"];
    if (typeof sub !== "string" || !sub) return { ok: false, reason: "no subject" };
    return { ok: true, subject: sub, claims };
  }
}
