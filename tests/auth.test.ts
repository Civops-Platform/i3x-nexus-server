// Sign-in: OAuth 2 access tokens (JWT, JWKS) and client certificates (mTLS).

import { test } from "node:test";
import assert from "node:assert/strict";
import http from "node:http";
import https from "node:https";
import { execFileSync } from "node:child_process";
import { generateKeyPairSync, sign, createHmac, type KeyObject } from "node:crypto";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { AddressInfo } from "node:net";
import { Nexus } from "../src/nexus";
import { sha256, type PrincipalDef } from "../src/core/auth";
import { exampleConfig } from "./helpers";

const ISS = "https://idp.example.test", AUD = "urn:i3x-nexus:example-riverside";
const SIGNALS = "https://example.com/i3x/signals/1.0";

function keypair(kind: "rsa" | "ec", kid: string) {
  const { publicKey, privateKey } = kind === "rsa" ? generateKeyPairSync("rsa", { modulusLength: 2048 }) : generateKeyPairSync("ec", { namedCurve: "P-256" });
  return { kid, privateKey, jwk: { ...publicKey.export({ format: "jwk" }), kid, use: "sig" } };
}

function jwt(alg: string, kid: string, key: KeyObject | Buffer | null, claims: Record<string, unknown>) {
  const enc = (o: object) => Buffer.from(JSON.stringify(o)).toString("base64url");
  const data = `${enc({ alg, kid, typ: "JWT" })}.${enc(claims)}`;
  let sig = "";
  if (alg === "RS256") sig = sign("sha256", Buffer.from(data), key as KeyObject).toString("base64url");
  if (alg === "PS256") sig = sign("sha256", Buffer.from(data), { key: key as KeyObject, padding: 6, saltLength: 32 }).toString("base64url");
  if (alg === "ES256") sig = sign("sha256", Buffer.from(data), { key: key as KeyObject, dsaEncoding: "ieee-p1363" }).toString("base64url");
  if (alg === "HS256") sig = createHmac("sha256", key as Buffer).update(data).digest("base64url");
  return `${data}.${sig}`;
}

const now = () => Math.floor(Date.now() / 1000);
const claims = (o: Record<string, unknown> = {}) => ({ iss: ISS, aud: AUD, sub: "client-mes", exp: now() + 300, iat: now(), ...o });

test("OAuth 2: valid tokens of three algorithms; every forgery and mismatch refused; roles come from the nexus, not the token", { timeout: 30_000 }, async () => {
  const rsa = keypair("rsa", "rsa-1"), ec = keypair("ec", "ec-1"), rsa2 = keypair("rsa", "rsa-2");
  let jwks: object[] = [rsa.jwk, ec.jwk];
  const idp = http.createServer((_q, r) => { r.writeHead(200, { "content-type": "application/json" }); r.end(JSON.stringify({ keys: jwks })); });
  await new Promise<void>((r) => idp.listen(0, "127.0.0.1", r));
  const jwksUrl = `http://127.0.0.1:${(idp.address() as AddressInfo).port}/jwks`;
  const dir = mkdtempSync(join(tmpdir(), "nexus-auth-"));
  const principals: PrincipalDef[] = [
    { id: "svc:mes", kind: "service", scopes: ["read"], owns: [SIGNALS], roles: [], oauthSubject: "client-mes" },
    { id: "reader", kind: "person", scopes: ["read"], owns: [], tokenSha256: sha256("opaque-reader") },
  ];
  const n = new Nexus({ config: exampleConfig(), principals, dataDir: dir, port: 0, rate: { perSecond: 5000, burst: 5000 }, oauth: { issuer: ISS, jwksUrl, audience: AUD } });
  const port = await n.start();
  const call = async (tok: string, method = "POST", path = "/objects/value", body: unknown = { elementIds: ["eq:line2-filler"] }) =>
    (await fetch(`http://127.0.0.1:${port}/v1${path}`, { method, headers: { authorization: `Bearer ${tok}`, "content-type": "application/json" }, body: JSON.stringify(body) })).status;
  try {
    assert.equal(await call(jwt("RS256", "rsa-1", rsa.privateKey, claims())), 200);
    assert.equal(await call(jwt("ES256", "ec-1", ec.privateKey, claims())), 200);
    assert.equal(await call(jwt("PS256", "rsa-1", rsa.privateKey, claims())), 200);
    // Writes as the registered principal: it owns the signals namespace.
    const w = await fetch(`http://127.0.0.1:${port}/v1/objects/value`, { method: "PUT", headers: { authorization: `Bearer ${jwt("RS256", "rsa-1", rsa.privateKey, claims())}`, "content-type": "application/json" },
      body: JSON.stringify({ updates: [{ elementId: "signal:ship-eta", value: { value: { status: "active", order: "SO-OIDC" } } }] }) });
    assert.equal(((await w.json()) as any).success, true);

    const refused: Array<[string, string]> = [
      [jwt("RS256", "rsa-1", rsa.privateKey, claims({ exp: now() - 3600 })), "expired"],
      [jwt("RS256", "rsa-1", rsa.privateKey, claims({ aud: "someone-else" })), "wrong audience"],
      [jwt("RS256", "rsa-1", rsa.privateKey, claims({ iss: "https://evil.example" })), "wrong issuer"],
      [jwt("RS256", "rsa-1", rsa.privateKey, claims({ sub: "unknown-client" })), "subject not registered"],
      [jwt("RS256", "rsa-1", rsa.privateKey, claims({ nbf: now() + 3600 })), "not yet valid"],
      [jwt("RS256", "rsa-1", rsa.privateKey, claims()).replace(/\.[^.]+\./, `.${Buffer.from(JSON.stringify(claims({ sub: "client-mes", admin: true }))).toString("base64url")}.`), "tampered payload"],
      [jwt("none", "rsa-1", null, claims()).replace(/[^.]*$/, ""), "alg none"],
      [jwt("HS256", "rsa-1", Buffer.from(JSON.stringify(rsa.jwk)), claims()), "HMAC with the public key"],
      [jwt("ES256", "rsa-1", ec.privateKey, claims()), "algorithm and key type mismatch"],
      [jwt("RS256", "rsa-2", rsa2.privateKey, claims()), "key the IdP has not published"],
    ];
    for (const [tok, why] of refused) assert.equal(await call(tok), 401, why);

    // Key rotation: the IdP publishes a new key; a token signed with it works after one refetch.
    jwks = [rsa2.jwk];
    assert.equal(await call(jwt("RS256", "rsa-2", rsa2.privateKey, claims())), 200, "rotated key picked up");

    // A token claiming roles grants nothing: svc:mes has no approving role.
    const roleClaims = jwt("RS256", "rsa-2", rsa2.privateKey, claims({ roles: ["plant-manager"], nexus_owns: ["https://example.com/i3x/conclusions/1.0"] }));
    const r = await fetch(`http://127.0.0.1:${port}/v1/objects/value`, { method: "PUT", headers: { authorization: `Bearer ${roleClaims}`, "content-type": "application/json" },
      body: JSON.stringify({ updates: [{ elementId: "decision:line2-speed-trial", value: { value: { status: "open" } } }] }) });
    assert.equal(((await r.json()) as any).results[0].responseDetail.status, 403, "ownership comes from the nexus registration");
    assert.equal(await call("opaque-reader"), 200, "opaque tokens still work beside JWTs");
  } finally {
    await n.stop(); idp.close(); rmSync(dir, { recursive: true, force: true });
  }
});

function makePki(dir: string) {
  const o = (args: string[]) => execFileSync("openssl", args, { cwd: dir, stdio: "pipe" });
  o(["req", "-x509", "-newkey", "rsa:2048", "-nodes", "-keyout", "ca.key", "-out", "ca.crt", "-days", "2", "-subj", "/CN=Example Site CA"]);
  o(["req", "-x509", "-newkey", "rsa:2048", "-nodes", "-keyout", "rogue.key", "-out", "rogue.crt", "-days", "2", "-subj", "/CN=Rogue CA"]);
  const leaf = (name: string, cn: string, ca: string, ext: string) => {
    o(["req", "-newkey", "rsa:2048", "-nodes", "-keyout", `${name}.key`, "-out", `${name}.csr`, "-subj", `/CN=${cn}`]);
    writeFileSync(join(dir, `${name}.ext`), ext);
    o(["x509", "-req", "-in", `${name}.csr`, "-CA", `${ca}.crt`, "-CAkey", `${ca}.key`, "-CAcreateserial", "-out", `${name}.crt`, "-days", "2", "-extfile", `${name}.ext`]);
  };
  leaf("server", "localhost", "ca", "subjectAltName=IP:127.0.0.1,DNS:localhost\nextendedKeyUsage=serverAuth\n");
  leaf("client", "svc:historian-bridge", "ca", "extendedKeyUsage=clientAuth\n");
  leaf("forged", "svc:historian-bridge", "rogue", "extendedKeyUsage=clientAuth\n");
  const r = (f: string) => readFileSync(join(dir, f), "utf8");
  return { ca: r("ca.crt"), server: { cert: r("server.crt"), key: r("server.key") }, client: { cert: r("client.crt"), key: r("client.key") }, forged: { cert: r("forged.crt"), key: r("forged.key") } };
}

test("mTLS: a certificate from the site CA identifies its principal; a forged one does not; HTTPS for everyone", { timeout: 30_000 }, async () => {
  const dir = mkdtempSync(join(tmpdir(), "nexus-mtls-"));
  const pki = makePki(dir);
  const n = new Nexus({
    config: exampleConfig(), dataDir: join(dir, "data"), port: 0, rate: { perSecond: 5000, burst: 5000 },
    tls: { cert: pki.server.cert, key: pki.server.key, ca: pki.ca },
    principals: [
      { id: "svc:historian-bridge", kind: "service", scopes: ["read"], owns: [SIGNALS], certSubject: "svc:historian-bridge" },
      { id: "reader", kind: "person", scopes: ["read"], owns: [], tokenSha256: sha256("opaque-reader") },
    ],
  });
  const port = await n.start();
  const req = (path: string, opts: { cert?: { cert: string; key: string }; token?: string; method?: string; body?: unknown } = {}) =>
    new Promise<{ status: number; body: any }>((resolve, reject) => {
      const r = https.request({ host: "127.0.0.1", port, path: `/v1${path}`, method: opts.method ?? "POST", ca: pki.ca, ...(opts.cert ?? {}),
        headers: { "content-type": "application/json", ...(opts.token ? { authorization: `Bearer ${opts.token}` } : {}) } }, (res) => {
        let s = ""; res.on("data", (d) => (s += d)); res.on("end", () => resolve({ status: res.statusCode!, body: s ? JSON.parse(s) : null }));
      });
      r.on("error", reject);
      r.end(opts.method === "GET" ? undefined : JSON.stringify(opts.body ?? { elementIds: ["eq:line2-filler"] }));
    });
  try {
    assert.equal((await req("/info", { method: "GET" })).status, 200, "/info needs no credential");
    assert.equal((await req("/objects/value", { cert: pki.client })).status, 200, "site-CA certificate identifies svc:historian-bridge");
    const w = await req("/objects/value", { cert: pki.client, method: "PUT", body: { updates: [{ elementId: "signal:ship-eta", value: { value: { status: "active", order: "SO-MTLS" } } }] } });
    assert.equal(w.body.success, true, "and it may write what that principal owns");
    assert.equal((await req("/objects/value", { cert: pki.forged })).status, 401, "same subject, wrong CA: nobody");
    assert.equal((await req("/objects/value", {})).status, 401, "no credential");
    assert.equal((await req("/objects/value", { token: "opaque-reader" })).status, 200, "bearer over HTTPS still works");
  } finally {
    await n.stop(); rmSync(dir, { recursive: true, force: true });
  }
});

test("forwarded identity: the governor accepts a standby's assertion only with the cluster secret, briefly", async () => {
  const { Authenticator } = await import("../src/core/auth");
  const defs: PrincipalDef[] = [{ id: "svc:historian-bridge", kind: "service", scopes: ["read"], owns: [], certSubject: "svc:historian-bridge" }];
  const standby = new Authenticator(defs, null, "cluster-secret");
  const governor = new Authenticator(defs, null, "cluster-secret");
  const other = new Authenticator(defs, null, "another-secret");
  const a = standby.assertForwarded("svc:historian-bridge");
  assert.equal((await governor.fromRequest({ forwardedPrincipal: a }))?.id, "svc:historian-bridge");
  assert.equal(await other.fromRequest({ forwardedPrincipal: a }), null, "a different cluster's secret");
  assert.equal(await governor.fromRequest({ forwardedPrincipal: a.replace(/^[^.]+/, Buffer.from("admin").toString("base64url")) }), null, "a swapped principal");
  assert.equal(await governor.fromRequest({ forwardedPrincipal: standby.assertForwarded("svc:historian-bridge", Date.now() - 60_000) }), null, "expired");
});
