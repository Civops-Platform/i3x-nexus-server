import { test } from "node:test";
import assert from "node:assert/strict";
import { createHash, randomBytes } from "node:crypto";
import { startNexus, TOKENS } from "./helpers";

const img = randomBytes(5000);
const sha = createHash("sha256").update(img).digest("hex");
const id = `nexus:media:${sha}`;

test("media: request → signed upload → hash-verified → available, related to its camera and alarm", async () => {
  const n = await startNexus();
  try {
    const req = { sha256: sha, mimeType: "image/jpeg", size: img.length, capturedBy: "cam:line2-filler", documents: "alarm:line2-filler-high-temp", capturedAt: "2026-10-30T12:00:00Z" };
    const denied = await n.call("PUT", "/objects/value", { updates: [{ elementId: "nexus:intake:media", value: { value: req } }] }, TOKENS.reader);
    assert.equal(denied.body.results[0].responseDetail.status, 403, "uploads need media:upload");
    const w = await n.call("PUT", "/objects/value", { updates: [{ elementId: "nexus:intake:media", value: { value: req } }] }, TOKENS.uploader);
    assert.equal(w.body.success, true);

    let v = await n.value(id);
    assert.equal(v.result.value.status, "awaiting_bytes");
    const putUrl: string = v.result.value.href;

    // Wrong bytes are discarded; the item stays awaiting.
    const bad = await fetch(putUrl, { method: "PUT", body: randomBytes(img.length) });
    assert.equal(bad.status, 422);
    assert.equal((await n.value(id)).result.value.status, "awaiting_bytes");
    // Tampered signature is refused.
    assert.equal((await fetch(putUrl.replace(/sig=[^&]+/, "sig=AAAA"), { method: "PUT", body: img })).status, 403);

    assert.equal((await fetch(putUrl, { method: "PUT", body: img })).status, 204);
    v = await n.value(id);
    assert.equal(v.result.value.status, "available");
    const got = Buffer.from(await (await fetch(v.result.value.href)).arrayBuffer());
    assert.equal(createHash("sha256").update(got).digest("hex"), sha);

    const rel = await n.call("POST", "/objects/related", { elementIds: ["cam:line2-filler", "alarm:line2-filler-high-temp"] });
    assert.ok(rel.body.results[0].result.some((e: any) => e.sourceRelationship === "Captured" && e.object.elementId === id));
    assert.ok(rel.body.results[1].result.some((e: any) => e.sourceRelationship === "DocumentedBy" && e.object.elementId === id));

    // Idempotent: the same request again names the same item and creates nothing new.
    await n.call("PUT", "/objects/value", { updates: [{ elementId: "nexus:intake:media", value: { value: req } }] }, TOKENS.uploader);
    const list = await n.call("GET", "/objects?typeElementId=MediaItem");
    assert.equal(list.body.result.length, 1);
  } finally { await n.stop(); }
});

test("media: refused types and sizes", async () => {
  const n = await startNexus();
  try {
    const bad = [
      { sha256: sha, mimeType: "application/x-msdownload", size: 10 },
      { sha256: "XYZ", mimeType: "image/png", size: 10 },
      { sha256: sha, mimeType: "image/png", size: 10 ** 12 },
      { sha256: sha, mimeType: "image/png", size: 10, capturedBy: "cam:nowhere" },
    ];
    const r = await n.call("PUT", "/objects/value", { updates: bad.map((v) => ({ elementId: "nexus:intake:media", value: { value: v } })) }, TOKENS.uploader);
    assert.deepEqual(r.body.results.map((x: any) => x.responseDetail?.status), [400, 400, 400, 400]);
  } finally { await n.stop(); }
});
