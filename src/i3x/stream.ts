// `POST /subscriptions/stream` on a long-lived process.
//
// An earlier serverless implementation had to poll a persisted store every 2 s because it ran on
// serverless hosting. A site nexus is a process that stays up, so the stream is
// driven by the cache instead:
//
//   - P1 values are flushed on a short tick (default 250 ms). Between ticks only
//     the latest value per element is kept, which is the conflation of spec §8.1:
//     a slow client loses intermediate values, never the current one.
//   - A change to a P0 element (alarm, quality change) wakes the stream at once
//     and is written before anything else in that flush.
//
// Single-stream-per-subscription is the same generation check as before: opening
// a stream bumps the subscription's generation and a stale stream closes itself.

import type { ServerResponse } from "node:http";
import { changesSince, type FabricSource, type Subscription, type SyncUpdate } from "./wire";

export interface StreamDeps {
  res: ServerResponse;
  sub: () => Subscription | null;
  clientId: string;
  generation: number;
  pending: SyncUpdate[];
  /** P0 events since the client's Last-Event-ID, replayed before anything else (resume after failover). */
  replay?: SyncUpdate[];
  lastSeen: Array<[string, string]>;
  fabric: FabricSource;
  isP0: (elementId: string) => boolean;
  /** Register a waker for P0 changes; returns the unregister function. */
  onUrgent: (wake: (elementId: string) => void) => () => void;
  tickMs?: number;
  keepaliveMs?: number;
}

export const SSE_HEADERS = {
  "Content-Type": "text/event-stream",
  "Cache-Control": "no-cache, no-transform",
  Connection: "keep-alive",
  "X-Accel-Buffering": "no",
};

// Each frame carries an SSE id (the send time). A client that reconnects, to this
// node or its partner, sends it back as Last-Event-ID and gets the P0 events it
// missed (profile NX-54). A client that ignores ids loses nothing it had before.
const frame = (updates: SyncUpdate[]) => `id: ${new Date().toISOString()}\ndata: ${JSON.stringify(updates)}\n\n`;

export function openStream(d: StreamDeps) {
  const lastSeen = new Map(d.lastSeen);
  let done = false;
  let lastWrite = Date.now();
  d.res.writeHead(200, SSE_HEADERS);
  d.res.write(": i3x stream open\n\n");
  if (d.replay?.length) d.res.write(frame(d.replay));
  if (d.pending.length) d.res.write(frame(d.pending));

  const flush = (urgentOnly: boolean) => {
    if (done) return;
    const s = d.sub();
    if (!s || s.clientId !== d.clientId || s.streamGeneration !== d.generation) return close();
    const ids = urgentOnly ? [...s.monitored.keys()].filter(d.isP0) : [...s.monitored.keys()];
    const updates = changesSince(d.fabric, ids, lastSeen);
    // P0 first within a frame, so a client that reads frames in order sees the alarm first.
    updates.sort((a, b) => Number(d.isP0(b.elementId)) - Number(d.isP0(a.elementId)));
    if (updates.length) { d.res.write(frame(updates)); lastWrite = Date.now(); }
    else if (Date.now() - lastWrite > (d.keepaliveMs ?? 15_000)) { d.res.write(": keepalive\n\n"); lastWrite = Date.now(); }
  };

  const timer = setInterval(() => flush(false), d.tickMs ?? 250);
  const unwake = d.onUrgent((id) => {
    const s = d.sub();
    if (s?.monitored.has(id)) flush(true);
  });
  const close = () => {
    if (done) return;
    done = true;
    clearInterval(timer);
    unwake();
    try { d.res.end(); } catch { /* client already gone */ }
  };
  d.res.on("close", close);
  return close;
}
