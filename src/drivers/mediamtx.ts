// Stream health from the media gateway as realtime values. Addresses:
//
//   <path>#online        boolean: the gateway is receiving the device
//   <path>#bitrateKbps   number: from bytes received between polls
//   <path>#viewers       integer: readers connected now
//   <path>#tracks        string: e.g. "H264,MPEG-4 Audio"
//
// A gateway that does not answer marks every value Bad. A path that exists but is
// not receiving reads online=false, Good: that is a true statement about the camera.

import type { Binding, SourceDef } from "../model/config";
import type { Driver, DriverSink } from "./driver";
import type { MediaMtxGateway } from "../media/gateway";

export class MediaMtxDriver implements Driver {
  private timer: NodeJS.Timeout | null = null;
  private connected = false;
  private detail = "not started";
  private last = new Map<string, { bytes: number; at: number }>();
  constructor(readonly source: SourceDef, private gw: () => MediaMtxGateway | null) {}

  async start(points: Array<{ attributeId: string; binding: Binding }>, sink: DriverSink) {
    const byPath = new Map<string, Array<{ attributeId: string; field: string }>>();
    for (const { attributeId, binding } of points) {
      const [path, field] = binding.address.split("#");
      byPath.set(path, [...(byPath.get(path) ?? []), { attributeId, field }]);
    }
    const every = (this.source.options as { pollMs?: number } | undefined)?.pollMs ?? 2_000;
    const poll = async () => {
      const gw = this.gw();
      const now = Date.now();
      const ts = new Date(now).toISOString();
      try {
        if (!gw) throw new Error("no gateway configured");
        for (const [path, items] of byPath) {
          const s = await gw.status(path);
          const prev = this.last.get(path);
          const kbps = s && prev && now > prev.at ? Math.round(((s.bytesReceived - prev.bytes) * 8) / (now - prev.at)) : null;
          if (s) this.last.set(path, { bytes: s.bytesReceived, at: now });
          for (const it of items) {
            const v = it.field === "online" ? !!s?.ready
              : it.field === "bitrateKbps" ? (s?.ready ? kbps : 0)
              : it.field === "viewers" ? (s?.readers ?? 0)
              : it.field === "tracks" ? (s?.tracks.join(",") ?? "")
              : null;
            sink.value(it.attributeId, v as never, v === null ? "Uncertain" : "Good", ts);
          }
        }
        if (!this.connected) { this.connected = true; this.detail = "connected"; sink.restored(); }
      } catch (e) {
        if (this.connected || this.detail === "not started") sink.lost((e as Error).message);
        this.connected = false;
        this.detail = (e as Error).message;
      }
    };
    await poll();
    this.timer = setInterval(() => void poll(), every);
  }

  async stop() { if (this.timer) clearInterval(this.timer); }
  status() { return { connected: this.connected, detail: this.detail }; }
}
