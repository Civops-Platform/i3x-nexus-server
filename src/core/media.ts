// Media: images, clips and audio (spec §5.4).
//
// i3X carries the reference; the bytes move out of band. A MediaItem's element id
// is derived from its content hash (`<prefix>:media:<sha256>`), which buys three
// things at once: a retried upload request finds the same item, a cited frame is
// provably the frame that was cited, and the store never holds the same bytes
// twice.
//
// Upload, in i3X terms:
//   1. PUT /objects/value on the media intake with { sha256, mimeType, size, ... }.
//      The item appears with status "awaiting_bytes" and a short-lived upload URL.
//   2. PUT the bytes to that URL. The hash is checked as they land; a mismatch is
//      discarded and the item stays awaiting.
//   3. The item reads "available" with a short-lived download URL.
//
// URLs are signed with an HMAC over (sha, verb, expiry), so the media routes need
// no session and a leaked URL dies in minutes.

import { createHash, createHmac, randomBytes, timingSafeEqual } from "node:crypto";
import { createWriteStream, existsSync, mkdirSync, renameSync, rmSync, statSync } from "node:fs";
import { join } from "node:path";
import { Readable } from "node:stream";
import type pg from "pg";

/** What a redundant pair shares about media: the index in Postgres, the bytes on each node. */
export interface MediaCluster {
  pool: pg.Pool;
  nodeId: string;
  /** Base URL (incl. /v1) of a peer node, or null if it is not alive. */
  peerUrl: (nodeId: string) => string | null;
  /** Shared secret for node-to-node byte fetches. */
  secret: string;
}

export interface MediaRequest {
  sha256: string;
  mimeType: string;
  size: number;
  capturedAt?: string;
  /** The camera or microphone that captured it. */
  capturedBy?: string;
  /** What it documents: an alarm, a work order, an observation. */
  documents?: string;
  durationMs?: number;
  width?: number;
  height?: number;
  description?: string;
}

export interface MediaItem extends MediaRequest {
  status: "awaiting_bytes" | "available";
  requestedBy: string;
  requestedAt: string;
}

export const ALLOWED_MIME = /^(image\/(jpeg|png|webp|tiff)|video\/(mp4|webm)|audio\/(wav|x-wav|mpeg|ogg|flac|webm))$/;
const SHA = /^[0-9a-f]{64}$/;

export function validateMediaRequest(v: unknown, maxBytes: number): { ok: true; req: MediaRequest } | { ok: false; detail: string } {
  if (!v || typeof v !== "object" || Array.isArray(v)) return { ok: false, detail: "value must be an object" };
  const r = v as Record<string, unknown>;
  if (typeof r.sha256 !== "string" || !SHA.test(r.sha256)) return { ok: false, detail: "sha256 must be 64 lowercase hex characters" };
  if (typeof r.mimeType !== "string" || !ALLOWED_MIME.test(r.mimeType)) return { ok: false, detail: `mimeType ${String(r.mimeType)} is not an accepted image, video or audio type` };
  if (typeof r.size !== "number" || !Number.isInteger(r.size) || r.size <= 0) return { ok: false, detail: "size must be a positive integer (bytes)" };
  if (r.size > maxBytes) return { ok: false, detail: `size ${r.size} exceeds the ${maxBytes}-byte limit` };
  for (const k of ["capturedAt", "capturedBy", "documents", "description"]) {
    if (r[k] !== undefined && typeof r[k] !== "string") return { ok: false, detail: `${k} must be a string` };
  }
  for (const k of ["durationMs", "width", "height"]) {
    if (r[k] !== undefined && typeof r[k] !== "number") return { ok: false, detail: `${k} must be a number` };
  }
  return { ok: true, req: r as unknown as MediaRequest };
}

export class MediaStore {
  private items = new Map<string, MediaItem>();
  private holders = new Map<string, string[]>();
  private key: Buffer;
  private cluster: MediaCluster | null = null;
  private syncTimer: NodeJS.Timeout | null = null;
  /** Called when the set of items changes (the address space must be rebuilt). */
  onIndexChange: () => void = () => {};

  constructor(private dir: string, signingKey?: string, public maxBytes = 512 * 1024 * 1024) {
    mkdirSync(join(dir, "incoming"), { recursive: true });
    this.key = signingKey ? Buffer.from(signingKey) : randomBytes(32);
  }

  get(sha: string) { return this.items.get(sha); }
  all() { return [...this.items.values()]; }

  /** Step 1. Idempotent: the same hash returns the existing item. */
  async request(req: MediaRequest, principal: string): Promise<MediaItem> {
    const existing = this.items.get(req.sha256);
    if (existing) return existing;
    const status = existsSync(this.path(req.sha256)) ? "available" : "awaiting_bytes";
    let item: MediaItem = { ...req, status, requestedBy: principal, requestedAt: new Date().toISOString() };
    if (this.cluster) {
      const { pool, nodeId } = this.cluster;
      await pool.query(`INSERT INTO nexus_media (sha, item, holders) VALUES ($1, $2, $3) ON CONFLICT (sha) DO NOTHING`,
        [req.sha256, JSON.stringify(item), status === "available" ? [nodeId] : []]);
      const r = await pool.query(`SELECT item, holders FROM nexus_media WHERE sha = $1`, [req.sha256]);
      item = r.rows[0].item as MediaItem;
      this.holders.set(req.sha256, r.rows[0].holders as string[]);
    }
    this.items.set(req.sha256, item);
    this.onIndexChange();
    return item;
  }

  /** Join a redundant pair: share the index, replicate bytes from whichever node holds them. */
  async attachCluster(c: MediaCluster, everyMs = 1_000) {
    this.cluster = c;
    await this.syncIndex();
    this.detached = false;
    const loop = async () => {
      await this.syncIndex().catch(() => {});
      await this.replicate().catch(() => {});
      if (!this.detached) this.syncTimer = setTimeout(loop, everyMs);
    };
    this.syncTimer = setTimeout(loop, everyMs);
  }

  private detached = false;
  detach() { this.detached = true; if (this.syncTimer) clearTimeout(this.syncTimer); this.syncTimer = null; }

  holdersOf(sha: string) { return this.holders.get(sha) ?? []; }
  hasLocal(sha: string) { return existsSync(this.path(sha)); }

  private async syncIndex() {
    const r = await this.cluster!.pool.query(`SELECT sha, item, holders FROM nexus_media`);
    let changed = false;
    for (const row of r.rows) {
      const prev = this.items.get(row.sha);
      const item = row.item as MediaItem;
      if (!prev || prev.status !== item.status) changed = true;
      this.items.set(row.sha, item);
      this.holders.set(row.sha, row.holders as string[]);
    }
    if (changed) this.onIndexChange();
  }

  /** Fetch bytes this node lacks from a node that has them, checking the hash on arrival (RPO for media, §9). */
  private async replicate() {
    const c = this.cluster!;
    for (const [sha, item] of this.items) {
      if (item.status !== "available" || this.hasLocal(sha)) continue;
      for (const h of this.holdersOf(sha)) {
        if (h === c.nodeId) continue;
        const ok = await this.pullFrom(h, sha).catch(() => false);
        if (ok) break;
      }
    }
  }

  /** A readable of the bytes from a peer, for a client GET on a node that has not replicated them yet. */
  async peerStream(sha: string): Promise<Readable | null> {
    const c = this.cluster;
    if (!c) return null;
    for (const h of this.holdersOf(sha)) {
      if (h === c.nodeId) continue;
      const base = c.peerUrl(h);
      if (!base) continue;
      const r = await fetch(`${base}/x-nexus/peer/media/${sha}`, { headers: this.peerHeaders(sha) }).catch(() => null);
      if (r?.ok && r.body) return Readable.fromWeb(r.body as never);
    }
    return null;
  }

  private async pullFrom(holder: string, sha: string): Promise<boolean> {
    const s = await this.peerStreamFrom(holder, sha);
    if (!s) return false;
    const item = this.items.get(sha)!;
    const r = await this.store(sha, item.size, s);
    if (!r.ok) return false;
    await this.cluster!.pool.query(`UPDATE nexus_media SET holders = array_append(holders, $2) WHERE sha = $1 AND NOT ($2 = ANY(holders))`, [sha, this.cluster!.nodeId]);
    return true;
  }

  private async peerStreamFrom(holder: string, sha: string): Promise<Readable | null> {
    const base = this.cluster!.peerUrl(holder);
    if (!base) return null;
    const r = await fetch(`${base}/x-nexus/peer/media/${sha}`, { headers: this.peerHeaders(sha) });
    return r.ok && r.body ? Readable.fromWeb(r.body as never) : null;
  }

  peerHeaders(sha: string, now = Date.now()) {
    const exp = String(Math.floor(now / 1000) + 60);
    const sig = createHmac("sha256", this.cluster!.secret).update(`peer|${sha}|${exp}`).digest("base64url");
    return { "x-nexus-peer-exp": exp, "x-nexus-peer-sig": sig };
  }

  verifyPeer(sha: string, exp: string | undefined, sig: string | undefined, now = Date.now()) {
    if (!this.cluster || !exp || !sig || !/^\d+$/.test(exp) || Number(exp) * 1000 < now) return false;
    const want = createHmac("sha256", this.cluster.secret).update(`peer|${sha}|${exp}`).digest();
    const got = Buffer.from(sig, "base64url");
    return got.length === want.length && timingSafeEqual(got, want);
  }

  /** Step 2. Streams to a temp file while hashing; only a matching hash is kept. */
  async receive(sha: string, body: Readable): Promise<{ ok: true } | { ok: false; status: number; detail: string }> {
    const item = this.items.get(sha);
    if (!item) return { ok: false, status: 404, detail: "no upload was requested for this hash" };
    if (item.status === "available" && this.hasLocal(sha)) return { ok: true };
    const r = await this.store(sha, item.size, body);
    if (!r.ok) return r;
    item.status = "available";
    if (this.cluster) {
      await this.cluster.pool.query(
        `UPDATE nexus_media SET item = jsonb_set(item, '{status}', '"available"'),
                holders = CASE WHEN $2 = ANY(holders) THEN holders ELSE array_append(holders, $2) END
          WHERE sha = $1`, [sha, this.cluster.nodeId]);
      this.holders.set(sha, [...new Set([...this.holdersOf(sha), this.cluster.nodeId])]);
    }
    this.onIndexChange();
    return { ok: true };
  }

  /** Stream bytes to disk while hashing; keep them only if hash and size match. */
  private async store(sha: string, declared: number, body: Readable): Promise<{ ok: true } | { ok: false; status: number; detail: string }> {
    const item = { size: declared };
    const tmp = join(this.dir, "incoming", `${sha}.${randomBytes(6).toString("hex")}`);
    const hash = createHash("sha256");
    const out = createWriteStream(tmp);
    let size = 0;
    try {
      for await (const chunk of body) {
        size += (chunk as Buffer).length;
        if (size > item.size) throw new Error(`more than the declared ${item.size} bytes`);
        hash.update(chunk as Buffer);
        if (!out.write(chunk)) await new Promise((r) => out.once("drain", r));
      }
      await new Promise<void>((res, rej) => out.end((e?: Error | null) => (e ? rej(e) : res())));
    } catch (e) {
      out.destroy();
      rmSync(tmp, { force: true });
      return { ok: false, status: 400, detail: `upload rejected: ${(e as Error).message}` };
    }
    const got = hash.digest("hex");
    if (got !== sha || size !== item.size) {
      rmSync(tmp, { force: true });
      return { ok: false, status: 422, detail: `bytes do not match the request (sha256 ${got}, ${size} bytes)` };
    }
    renameSync(tmp, this.path(sha));
    return { ok: true };
  }

  path(sha: string) { return join(this.dir, sha); }
  sizeOnDisk(sha: string) { return existsSync(this.path(sha)) ? statSync(this.path(sha)).size : 0; }

  sign(sha: string, verb: "get" | "put", ttlMs = 5 * 60_000, now = Date.now()) {
    const exp = Math.floor((now + ttlMs) / 1000);
    const sig = createHmac("sha256", this.key).update(`${sha}|${verb}|${exp}`).digest("base64url");
    return { exp, sig };
  }

  verify(sha: string, verb: "get" | "put", exp: string | null, sig: string | null, now = Date.now()) {
    if (!exp || !sig || !/^\d+$/.test(exp) || Number(exp) * 1000 < now) return false;
    const want = createHmac("sha256", this.key).update(`${sha}|${verb}|${exp}`).digest();
    const got = Buffer.from(sig, "base64url");
    return got.length === want.length && timingSafeEqual(got, want);
  }
}
