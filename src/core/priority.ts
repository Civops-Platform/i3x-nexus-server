// Priority lanes and admission control (spec §8).
//
// Each class has its own concurrency budget, so a flood of history queries cannot
// occupy the slots an approval or an alarm read needs. P0 has a reserved budget
// that no other class can borrow and is never refused for load. Below it, work is
// admitted while its lane has room; a full lane queues up to its queue cap and then
// refuses with Retry-After. Overall load (event-loop lag) tightens admission from
// the bottom: P3 is refused first, then P2.
//
// The server assigns the class (see `classify`); a caller cannot raise its own.

import { monitorEventLoopDelay } from "node:perf_hooks";
import type { PriorityClass } from "../model/config";

export interface LaneConfig { concurrency: number; queue: number }

export const DEFAULT_LANES: Record<PriorityClass, LaneConfig> = {
  P0: { concurrency: 16, queue: 1_000 },
  P1: { concurrency: 32, queue: 500 },
  P2: { concurrency: 12, queue: 200 },
  P3: { concurrency: 4, queue: 20 },
};

/** Event-loop lag (ms, p99 over the last second) at which each class stops being admitted. */
export const SHED_AT_LAG_MS: Record<PriorityClass, number> = { P0: Infinity, P1: 1_000, P2: 250, P3: 100 };

export type Admit = { ok: true; release: () => void } | { ok: false; retryAfterS: number; reason: string };

class Lane {
  active = 0;
  waiting: Array<() => void> = [];
  constructor(public cfg: LaneConfig) {}
}

export class Lanes {
  private lanes: Record<PriorityClass, Lane>;
  private lag = monitorEventLoopDelay({ resolution: 10 });
  private lagP99 = 0;
  private timer: NodeJS.Timeout;
  readonly stats: Record<PriorityClass, { admitted: number; shed: number }> = {
    P0: { admitted: 0, shed: 0 }, P1: { admitted: 0, shed: 0 }, P2: { admitted: 0, shed: 0 }, P3: { admitted: 0, shed: 0 },
  };

  constructor(cfg: Record<PriorityClass, LaneConfig> = DEFAULT_LANES, private lagOverride?: () => number) {
    this.lanes = { P0: new Lane(cfg.P0), P1: new Lane(cfg.P1), P2: new Lane(cfg.P2), P3: new Lane(cfg.P3) };
    this.lag.enable();
    this.timer = setInterval(() => { this.lagP99 = this.lag.percentile(99) / 1e6; this.lag.reset(); }, 1_000);
    this.timer.unref();
  }

  currentLagMs() { return this.lagOverride ? this.lagOverride() : this.lagP99; }

  async admit(cls: PriorityClass): Promise<Admit> {
    const lane = this.lanes[cls];
    if (cls !== "P0" && this.currentLagMs() >= SHED_AT_LAG_MS[cls]) {
      this.stats[cls].shed++;
      return { ok: false, retryAfterS: cls === "P3" ? 10 : 2, reason: `the nexus is under load and is serving higher-priority work first (${cls} paused)` };
    }
    if (lane.active < lane.cfg.concurrency) return this.grant(cls, lane);
    if (cls !== "P0" && lane.waiting.length >= lane.cfg.queue) {
      this.stats[cls].shed++;
      return { ok: false, retryAfterS: cls === "P3" ? 10 : 1, reason: `the ${cls} lane is full` };
    }
    await new Promise<void>((r) => lane.waiting.push(r));
    return this.grant(cls, lane);
  }

  private grant(cls: PriorityClass, lane: Lane): Admit {
    lane.active++;
    this.stats[cls].admitted++;
    let done = false;
    return {
      ok: true,
      release: () => {
        if (done) return;
        done = true;
        lane.active--;
        lane.waiting.shift()?.();
      },
    };
  }

  close() { clearInterval(this.timer); this.lag.disable(); }
}

/**
 * Which lane an i3X request runs in. History and media bytes are bulk (P3);
 * writes are business (P2); reads, subscriptions and streams are operations (P1)
 * unless every element they name is P0, which is how an alarm console's reads
 * stay fast when the node is busy.
 */
export function classify(method: string, path: string, elementClasses: PriorityClass[]): PriorityClass {
  if (path === "/info") return "P0";
  // Stopping agents must never queue behind the load they are causing.
  if (path === "/x-nexus/agents/freeze") return "P0";
  if (path.startsWith("/x-nexus/media/")) return "P3";
  if (path === "/objects/history" && method === "POST") return "P3";
  if (method === "PUT") return elementClasses.length && elementClasses.every((c) => c === "P0") ? "P0" : "P2";
  if (elementClasses.length && elementClasses.every((c) => c === "P0")) return "P0";
  return "P1";
}
