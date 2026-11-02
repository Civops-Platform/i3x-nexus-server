// The last-value cache: the current VQT of every realtime attribute (spec §4).
//
// Drivers write into it; the i3X projection and the subscription streams read it.
// Two rules from the spec live here because this is the one place every value
// passes through:
//
//   1. A value that stops arriving goes Uncertain after 3× its scan period, and a
//      source that is lost marks its attributes Bad. The last value is kept, so a
//      client sees "this was 71.2, and it is no longer live", never a fresh-looking
//      number.
//   2. Nothing is ever substituted. The reference server this design learned from
//      silently swapped in a mock dataset when its source failed; here a failed
//      source can only make values worse-quality, never different.

import { EventEmitter } from "node:events";
import type { VQT } from "../i3x/wire";

export type Quality = VQT["quality"];

export interface CachedValue {
  value: VQT["value"];
  quality: Quality;
  /** Source timestamp: when the value was true. Never rewritten. */
  timestamp: string;
  /** When the nexus received it (metadata, spec §5). */
  receivedAt: number;
  /** Quality as the source reported it, before staleness was applied. */
  sourceQuality: Quality;
}

export interface CacheChange { attributeId: string; current: CachedValue; changed: boolean }

export class LastValueCache extends EventEmitter {
  private values = new Map<string, CachedValue>();
  private scan = new Map<string, number>();
  private bySource = new Map<string, Set<string>>();

  /** Declare an attribute before its first value, so it reads GoodNoData, not 404. */
  declare(attributeId: string, sourceId: string, scanMs: number) {
    this.scan.set(attributeId, scanMs);
    if (!this.bySource.has(sourceId)) this.bySource.set(sourceId, new Set());
    this.bySource.get(sourceId)!.add(attributeId);
  }

  get(attributeId: string): CachedValue | undefined { return this.values.get(attributeId); }

  attributesOf(sourceId: string): string[] { return [...(this.bySource.get(sourceId) ?? [])]; }

  /** A value from a driver. Emits `change` with whether value or quality differs from before. */
  set(attributeId: string, value: VQT["value"], quality: Quality, timestamp: string, now = Date.now()) {
    const prev = this.values.get(attributeId);
    const current: CachedValue = { value, quality, timestamp, receivedAt: now, sourceQuality: quality };
    this.values.set(attributeId, current);
    const changed = !prev || prev.value !== value || prev.quality !== quality;
    this.emit("change", { attributeId, current, changed } satisfies CacheChange);
  }

  /** The source is gone: every attribute it feeds goes Bad, keeping its last value. */
  sourceLost(sourceId: string, now = Date.now()) {
    for (const id of this.attributesOf(sourceId)) this.degrade(id, "Bad", now);
  }

  /** Apply staleness. Call on a timer; returns how many attributes it degraded. */
  sweep(now = Date.now()): number {
    let n = 0;
    for (const [id, v] of this.values) {
      const scan = this.scan.get(id);
      if (!scan || v.quality !== "Good") continue;
      if (now - v.receivedAt > 3 * scan) { this.degrade(id, "Uncertain", now); n++; }
    }
    return n;
  }

  /** One attribute's quality changes with no new value: a Sparkplug seq gap, a device death. */
  mark(attributeId: string, quality: Quality, now = Date.now()) { this.degrade(attributeId, quality, now); }

  private degrade(attributeId: string, quality: Quality, _now: number) {
    const prev = this.values.get(attributeId);
    if (!prev) {
      // Never received a value: there is nothing to keep, and Bad with null is the truth.
      const current: CachedValue = { value: null, quality, timestamp: new Date(_now).toISOString(), receivedAt: _now, sourceQuality: quality };
      this.values.set(attributeId, current);
      this.emit("change", { attributeId, current, changed: true } satisfies CacheChange);
      return;
    }
    if (prev.quality === quality || prev.quality === "Bad" || (quality === "Uncertain" && prev.quality !== "Good")) return;
    const current = { ...prev, quality };
    this.values.set(attributeId, current);
    this.emit("change", { attributeId, current, changed: true } satisfies CacheChange);
  }
}
