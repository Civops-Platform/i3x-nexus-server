// Source drivers (spec §4). A driver turns one southbound source into VQTs in the
// last-value cache. It never invents a value: a lost connection is reported with
// `lost()`, which degrades every attribute the source feeds to Bad.

import type { Binding, SourceDef } from "../model/config";
import type { Quality } from "../core/cache";
import type { VQT } from "../i3x/wire";

export interface DriverSink {
  value(attributeId: string, value: VQT["value"], quality: Quality, sourceTimestamp: string): void;
  lost(reason: string): void;
  /** Mark one attribute's quality without a new value (last value kept; Bad clears it on read). */
  degrade?(attributeId: string, quality: Quality): void;
  restored(): void;
}

export interface Driver {
  readonly source: SourceDef;
  start(points: Array<{ attributeId: string; binding: Binding }>, sink: DriverSink): Promise<void>;
  stop(): Promise<void>;
  status(): { connected: boolean; detail: string };
  /**
   * The W2 conduit (profile NX-61): write one value to the device. Only drivers with a
   * write path implement it. `permanent` marks a refusal by the device (not writable,
   * out of range), as opposed to a failure that may succeed on retry.
   */
  write?(address: string, value: unknown): Promise<{ ok: true } | { ok: false; detail: string; permanent?: boolean }>;
}

/** OPC UA status code → i3X quality class. The severity bits are the top two of the 32-bit code. */
export function qualityOfStatus(code: number): Quality {
  const severity = (code >>> 30) & 0b11;
  if (severity === 0) return "Good";
  if (severity === 1) return "Uncertain";
  return "Bad";
}
