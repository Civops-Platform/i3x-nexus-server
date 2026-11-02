import { mkdtempSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Nexus, type NexusOptions } from "../src/nexus";
import { sha256, type PrincipalDef } from "../src/core/auth";
import type { SiteConfig } from "../src/model/config";

export const TOKENS = { reader: "tok-reader", publisher: "tok-publisher", uploader: "tok-uploader", agent: "tok-agent" };

export function principals(cfg: SiteConfig): PrincipalDef[] {
  const owned = [...new Set(cfg.types.filter((t) => t.write === "W0").map((t) => t.namespaceUri))];
  return [
    { id: "reader", kind: "person", scopes: ["read"], owns: [], tokenSha256: sha256(TOKENS.reader) },
    { id: "publisher", kind: "service", scopes: ["read"], owns: owned, tokenSha256: sha256(TOKENS.publisher) },
    { id: "uploader", kind: "service", scopes: ["read", "media:upload", "media:read"], owns: [], tokenSha256: sha256(TOKENS.uploader) },
    // An agent that owns only the signals namespace: it may not publish conclusions.
    { id: "agent:optimizer", kind: "agent", scopes: ["read"], owns: ["https://example.com/i3x/signals/1.0"], tokenSha256: sha256(TOKENS.agent) },
  ];
}

export const exampleConfig = (): SiteConfig => JSON.parse(readFileSync("config/example-site.json", "utf8"));

export async function startNexus(cfg: SiteConfig = exampleConfig(), extra: Partial<NexusOptions> = {}) {
  const dir = mkdtempSync(join(tmpdir(), "nexus-test-"));
  const nexus = new Nexus({ config: cfg, principals: principals(cfg), dataDir: dir, port: 0, rate: { perSecond: 1000, burst: 1000 }, ...extra });
  const port = await nexus.start();
  const base = `http://127.0.0.1:${port}/v1`;
  const call = async (method: string, path: string, body?: unknown, token = TOKENS.reader) => {
    const r = await fetch(base + path, {
      method, headers: { authorization: `Bearer ${token}`, "content-type": "application/json" },
      body: body === undefined ? undefined : JSON.stringify(body),
    });
    return { status: r.status, headers: r.headers, body: (await r.json().catch(() => null)) as any };
  };
  const value = async (id: string, token?: string) => (await call("POST", "/objects/value", { elementIds: [id] }, token)).body.results[0];
  const stop = async () => { await nexus.stop(); rmSync(dir, { recursive: true, force: true }); };
  return { nexus, base, call, value, stop, dir };
}

export async function until<T>(f: () => Promise<T | undefined | null | false>, ms = 8000, step = 100): Promise<T> {
  const end = Date.now() + ms;
  for (;;) {
    const v = await f();
    if (v) return v as T;
    if (Date.now() > end) throw new Error("condition not met in time");
    await new Promise((r) => setTimeout(r, step));
  }
}
