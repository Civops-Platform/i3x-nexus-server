// Live streams through a media gateway. Needs a running MediaMTX configured by
// config/test/mediamtx.yml and an RTSP camera; scripts/gateway-e2e-docker.sh sets both up.

import { test } from "node:test";
import assert from "node:assert/strict";
import net from "node:net";
import { randomBytes } from "node:crypto";
import { startNexus, until, TOKENS } from "./helpers";
import { exampleConfig } from "./helpers";

const skip = !process.env.NEXUS_TEST_GATEWAY && "no media gateway (run scripts/gateway-e2e-docker.sh)";

function rtspDescribe(url: string): Promise<number> {
  return new Promise((resolve, reject) => {
    const u = new URL(url);
    const s = net.connect(Number(u.port), u.hostname, () => s.write(`DESCRIBE ${url} RTSP/1.0\r\nCSeq: 1\r\nAccept: application/sdp\r\n\r\n`));
    let buf = "";
    s.on("data", (d) => { buf += d; const m = /^RTSP\/1\.0 (\d+)/.exec(buf); if (m) { s.destroy(); resolve(Number(m[1])); } });
    s.on("error", reject);
    setTimeout(() => { s.destroy(); reject(new Error("RTSP timeout")); }, 8000);
  });
}

test("gateway: stream health as values, signed viewer access, no publishing, alarm → clip MediaItem", { skip }, async () => {
  process.env.GW_USER = "nexus"; process.env.GW_PASS = randomBytes(16).toString("hex");
  const cfg = exampleConfig();
  cfg.mediaGateway = { id: "gw", kind: "mediamtx", apiUrl: "http://127.0.0.1:9997", playbackUrl: "http://127.0.0.1:9996", viewerBase: "http://127.0.0.1:8889", userEnv: "GW_USER", passEnv: "GW_PASS" };
  cfg.sources.push({ id: "gw", driver: "mediamtx", options: { pollMs: 1000 } });
  const t = cfg.types.find((x) => x.id === "MediaStream")!;
  t.attributes = [{ name: "codec", dataType: "string" }, { name: "resolution", dataType: "string" }, { name: "viewerUrl", dataType: "string" },
    { name: "online", dataType: "boolean" }, { name: "bitrateKbps", dataType: "number" }, { name: "viewers", dataType: "integer" }];
  const cam = cfg.objects.find((o) => o.id === "cam:line2-filler")!;
  cam.stream = { codec: "H.264", resolution: "640x360", path: "line2-filler", cameraUrl: process.env.NEXUS_TEST_CAMERA ?? "rtsp://127.0.0.1:8654/cam" };
  cam.bindings = Object.fromEntries(["online", "bitrateKbps", "viewers"].map((f) => [f, { source: "gw", address: `line2-filler#${f}` }]));
  cfg.objects.push({ id: "cam:dark", name: "Camera that sends nothing", typeId: "MediaStream", parentId: "wc:line2",
    stream: { codec: "H.264", resolution: "640x360", path: "dark", cameraUrl: "rtsp://127.0.0.1:8655/none" },
    bindings: Object.fromEntries(["online", "bitrateKbps", "viewers"].map((f) => [f, { source: "gw", address: `dark#${f}` }])) });
  cfg.clips = [{ trigger: "alarm:line2-filler-high-temp", stream: "cam:line2-filler", preS: 3, postS: 3 }];
  // The simulator would trip the alarm on its own schedule; the test trips it.
  cfg.objects.find((o) => o.id === "alarm:line2-filler-high-temp")!.bindings!.value = { source: "sim", address: "alarm:rate=0", scanMs: 1000 };

  const n = await startNexus(cfg, { port: 8440 } as never);
  try {
    const live = await until(async () => { const r = (await n.value("cam:line2-filler")).result; return r.value?.online === true && r.value.bitrateKbps > 0 && r; }, 20_000, 500);
    assert.equal(live.quality, "Good");
    const dark = (await n.value("cam:dark")).result.value;
    assert.equal(dark.online, false, "a camera that sends nothing reads offline, as a true value");

    const viewer = live.value.viewerUrl as string;
    assert.match(viewer, /^http:\/\/127\.0\.0\.1:8889\/line2-filler\/\?token=\d+\.[\w-]+$/);
    const token = new URL(viewer).searchParams.get("token");
    assert.equal(await rtspDescribe(`rtsp://127.0.0.1:8554/line2-filler?token=${token}`), 200, "the signed viewer token opens the stream");
    assert.equal(await rtspDescribe("rtsp://127.0.0.1:8554/line2-filler"), 401, "no token, no stream");
    assert.equal(await rtspDescribe(`rtsp://127.0.0.1:8554/dark?token=${token}`), 401, "a token is for its own stream only");

    // Recording needs some history before the alarm (preS) and a closed segment after it.
    await new Promise((r) => setTimeout(r, 8000));
    const tripped = new Date().toISOString();
    n.nexus.cache.set("alarm:line2-filler-high-temp/value", true, "Good", tripped);
    const clip = await until(async () => n.nexus.clipsCut.find((c) => c.at > tripped), 75_000, 500);
    assert.ok("mediaId" in clip, `clip cut: ${JSON.stringify(clip)}`);
    const item = (await n.value((clip as { mediaId: string }).mediaId)).result.value;
    assert.equal(item.status, "available");
    assert.equal(item.documents, "alarm:line2-filler-high-temp");
    assert.equal(item.capturedBy, "cam:line2-filler");
    const bytes = Buffer.from(await (await fetch(item.href)).arrayBuffer());
    assert.equal(bytes.subarray(4, 8).toString("latin1"), "ftyp", "an MP4 clip");
    const rel = await n.call("POST", "/objects/related", { elementIds: ["alarm:line2-filler-high-temp"] }, TOKENS.reader);
    assert.ok(rel.body.results[0].result.some((e: any) => e.sourceRelationship === "DocumentedBy"), "the alarm points at its clip");
    console.log(`# live bitrate ${live.value.bitrateKbps} kbps; clip ${item.size} bytes`);
  } finally {
    await n.stop();
  }
});
