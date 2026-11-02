// Ledger anchoring (Agent extension §6, NXA-7).
//
// The hash chain shows an edit, a removal or a reordering, but only to someone who
// trusts the head of the chain. Anyone with write access to the ledger's database
// could rewrite every version from the edit onward and leave a chain that verifies.
// So the governor signs the head every few minutes, with a key that is not in the
// database, and appends the checkpoint somewhere the database's administrators
// cannot rewrite: an append-only file on other storage, an HTTP endpoint in another
// trust domain, or both. A rewritten ledger then disagrees with a checkpoint it
// cannot forge (`scripts/verify-anchors.ts`).

import { appendFileSync, mkdirSync } from "node:fs";
import { dirname } from "node:path";
import { createPrivateKey, createPublicKey, sign, verify, type KeyObject } from "node:crypto";
import { canonical } from "./records";

export interface Checkpoint { site: string; node: string; count: number; head: string; at: string }
export interface Anchor { checkpoint: Checkpoint; sig: string; keyId: string }

export interface AnchorOptions {
  /** Ed25519 private key, PEM. */
  keyPem: string;
  /** Append-only JSON Lines file (on storage the ledger's administrators cannot rewrite). */
  file?: string;
  /** POSTed one anchor at a time, as JSON. */
  url?: string;
  everyMs?: number;
}

export const keyIdOf = (pub: KeyObject) => (pub.export({ format: "jwk" }) as { x: string }).x.slice(0, 16);

export function signCheckpoint(cp: Checkpoint, key: KeyObject): Anchor {
  const pub = createPublicKey(key);
  return { checkpoint: cp, sig: sign(null, Buffer.from(canonical(cp)), key).toString("base64url"), keyId: keyIdOf(pub) };
}

export function verifyAnchor(a: Anchor, pub: KeyObject): boolean {
  try { return verify(null, Buffer.from(canonical(a.checkpoint)), pub, Buffer.from(a.sig, "base64url")); } catch { return false; }
}

export class Anchorer {
  private key: KeyObject;
  private timer: NodeJS.Timeout | null = null;
  private last: string | null = null;
  readonly sent: Anchor[] = [];
  lastError: string | null = null;

  constructor(private o: AnchorOptions, private site: string, private node: string,
    private head: () => Promise<{ count: number; head: string }>, private mayWrite: () => boolean) {
    if (!o.file && !o.url) throw new Error("anchoring needs somewhere to send anchors: a file, a URL, or both");
    this.key = createPrivateKey(o.keyPem);
    if (o.file) mkdirSync(dirname(o.file), { recursive: true });
  }

  start() { this.timer = setInterval(() => void this.tick(), this.o.everyMs ?? 300_000); this.timer.unref(); }
  stop() { if (this.timer) clearInterval(this.timer); this.timer = null; }

  /** Anchor the current head if it moved since the last anchor. Only the governor anchors. */
  async tick(): Promise<Anchor | null> {
    if (!this.mayWrite()) return null;
    try {
      const h = await this.head();
      if (h.head === this.last) return null;
      const a = signCheckpoint({ site: this.site, node: this.node, count: h.count, head: h.head, at: new Date().toISOString() }, this.key);
      if (this.o.file) appendFileSync(this.o.file, JSON.stringify(a) + "\n");
      if (this.o.url) {
        const r = await fetch(this.o.url, { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify(a), signal: AbortSignal.timeout(10_000) });
        if (!r.ok) throw new Error(`anchor endpoint answered ${r.status}`);
      }
      this.last = h.head;
      this.sent.push(a);
      if (this.sent.length > 100) this.sent.shift();
      this.lastError = null;
      return a;
    } catch (e) {
      this.lastError = (e as Error).message;
      return null;
    }
  }
}

/**
 * Check a ledger (rows in write order) against anchors. A chain rewritten after an
 * anchor still verifies on its own, but its version at the anchored count no longer
 * has the anchored hash. Returns one line per anchor.
 */
export function checkAnchors(rows: Array<{ hash: string }>, anchors: Anchor[], pub: KeyObject) {
  return anchors.map((a) => {
    if (!verifyAnchor(a, pub)) return { at: a.checkpoint.at, count: a.checkpoint.count, ok: false, reason: "signature does not verify" };
    if (a.checkpoint.count > rows.length) return { at: a.checkpoint.at, count: a.checkpoint.count, ok: false, reason: `ledger has only ${rows.length} versions (versions were removed)` };
    const got = a.checkpoint.count === 0 ? "0".repeat(64) : rows[a.checkpoint.count - 1].hash;
    return got === a.checkpoint.head
      ? { at: a.checkpoint.at, count: a.checkpoint.count, ok: true, reason: null }
      : { at: a.checkpoint.at, count: a.checkpoint.count, ok: false, reason: "the ledger at this point differs from what was anchored (rewritten)" };
  });
}
