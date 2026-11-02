// A simulated source, for the conformance tenant and for tests. Addresses:
//
//   sine:period=60,amp=10,offset=50     smooth analog value (seconds)
//   ramp:min=0,max=100,period=30        sawtooth
//   random:min=0,max=1                  noise
//   step:values=Running|Idle|Fault,period=20   discrete state, cycles
//   bool:period=10                      toggles
//   alarm:rate=0.02                     true with that probability per scan
//
// It is labelled for what it is: the source's /info status says "simulator", so
// a simulated value can never be mistaken for a plant reading.

import type { Binding, SourceDef } from "../model/config";
import type { Driver, DriverSink } from "./driver";

type Gen = (tSec: number) => number | string | boolean;

export function parseSim(address: string): Gen {
  const [kind, rest = ""] = address.split(":");
  const p: Record<string, string> = {};
  for (const kv of rest.split(",").filter(Boolean)) { const [k, v] = kv.split("="); p[k] = v; }
  const n = (k: string, d: number) => (p[k] === undefined ? d : Number(p[k]));
  switch (kind) {
    case "sine": return (t) => round(n("offset", 50) + n("amp", 10) * Math.sin((2 * Math.PI * t) / n("period", 60)));
    case "ramp": return (t) => round(n("min", 0) + ((t % n("period", 30)) / n("period", 30)) * (n("max", 100) - n("min", 0)));
    case "random": return () => round(n("min", 0) + Math.random() * (n("max", 1) - n("min", 0)));
    case "step": { const vals = (p.values ?? "On|Off").split("|"); return (t) => vals[Math.floor(t / n("period", 20)) % vals.length]; }
    case "bool": return (t) => Math.floor(t / n("period", 10)) % 2 === 0;
    case "alarm": return () => Math.random() < n("rate", 0.02);
    default: throw new Error(`unknown simulator address ${address}`);
  }
}

const round = (x: number) => Math.round(x * 1000) / 1000;

export class SimulatorDriver implements Driver {
  private timers: NodeJS.Timeout[] = [];
  constructor(readonly source: SourceDef) {}

  async start(points: Array<{ attributeId: string; binding: Binding }>, sink: DriverSink) {
    const groups = new Map<number, Array<{ attributeId: string; gen: Gen }>>();
    for (const { attributeId, binding } of points) {
      const scan = binding.scanMs ?? 1000;
      if (!groups.has(scan)) groups.set(scan, []);
      groups.get(scan)!.push({ attributeId, gen: parseSim(binding.address) });
    }
    const t0 = Date.now();
    for (const [scan, items] of groups) {
      const tick = () => {
        const now = Date.now();
        const ts = new Date(now).toISOString();
        for (const it of items) sink.value(it.attributeId, it.gen((now - t0) / 1000), "Good", ts);
      };
      tick();
      this.timers.push(setInterval(tick, scan));
    }
  }

  async stop() { for (const t of this.timers) clearInterval(t); this.timers = []; }
  status() { return { connected: true, detail: "simulator" }; }
}
