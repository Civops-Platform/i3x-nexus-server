// Records of each system of record, projected into the address space (profile NX-6).
//
// Read-only and kept current by polling the adapter. Each record appears as
// `<system>:<record id>` under a folder per system, carrying the system's own
// version so a WriteRequest can be raised against exactly what a client saw. A
// change goes through the governor; a direct write to a projected record is
// refused with a pointer to the W1 intake.

import type { ObjectDef } from "../model/config";
import type { ExternalRecord, SystemAdapter } from "./adapters";

export const systemFolder = (id: string) => `nexus:system:${id}`;

export class SystemProjection {
  private recs = new Map<string, { system: string; rec: ExternalRecord }>();
  private timer: NodeJS.Timeout | null = null;
  private stopped = false;
  onStructureChange: () => void = () => {};

  constructor(private systems: Array<{ id: string; typeId: string; name?: string; adapter: SystemAdapter }>) {}

  async start(everyMs = 2_000) {
    await this.sync().catch(() => {});
    const loop = async () => { await this.sync().catch(() => {}); if (!this.stopped) this.timer = setTimeout(loop, everyMs); };
    this.timer = setTimeout(loop, everyMs);
  }

  stop() { this.stopped = true; if (this.timer) clearTimeout(this.timer); }

  async sync() {
    let changed = false;
    for (const s of this.systems) {
      const list = await s.adapter.list();
      const seen = new Set<string>();
      for (const rec of list) {
        const id = `${s.id}:${rec.id}`;
        seen.add(id);
        if (!this.recs.has(id)) changed = true;
        this.recs.set(id, { system: s.id, rec });
      }
      for (const id of [...this.recs.keys()]) if (this.recs.get(id)!.system === s.id && !seen.has(id)) { this.recs.delete(id); changed = true; }
    }
    if (changed) this.onStructureChange();
  }

  /** Refresh one record now (after a governed write), without waiting for the poll. */
  async refresh(system: string, recordId: string) {
    const s = this.systems.find((x) => x.id === system);
    const rec = await s?.adapter.read(recordId);
    if (!s || !rec) return;
    const id = `${system}:${recordId}`;
    const isNew = !this.recs.has(id);
    this.recs.set(id, { system, rec });
    if (isNew) this.onStructureChange();
  }

  objects(): ObjectDef[] {
    const out: ObjectDef[] = this.systems.map((s) => ({ id: systemFolder(s.id), name: s.name ?? s.id, typeId: "NexusFolder", parentId: null }));
    for (const [id, { system, rec }] of this.recs) {
      const s = this.systems.find((x) => x.id === system)!;
      out.push({ id, name: String(rec.fields.title ?? rec.id), typeId: s.typeId, parentId: systemFolder(system) });
    }
    return out;
  }

  value(elementId: string) {
    const r = this.recs.get(elementId);
    return r ? { fields: r.rec.fields, version: r.rec.version } : null;
  }

  resolve(elementId: string): { system: string; recordId: string | null } | null {
    for (const s of this.systems) if (elementId === systemFolder(s.id)) return { system: s.id, recordId: null };
    const r = this.recs.get(elementId);
    return r ? { system: r.system, recordId: r.rec.id } : null;
  }
}
