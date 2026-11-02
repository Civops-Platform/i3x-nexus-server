// Systems of record behind tier W1 (profile §7.1).
//
// An adapter is the only code that touches an external system. The governor asks it
// four things and nothing else: list the records (so they can be projected into the
// address space), read one (for the freshness check and the read-back), apply a
// change, and nothing more. Compensation is just another apply of the recorded
// prior state, so adapters need no undo of their own.
//
// `cmms-rest` speaks a plain REST work-order API:
//   GET  /workorders            → [{ id, version, ...fields }]
//   GET  /workorders/:id        → { id, version, ...fields }
//   PATCH /workorders/:id  If-Match: <version>  body: fields → { id, version, ... }  (409 on a stale version)
//   POST /workorders            body: fields → { id, version, ... }
// It is the shape most CMMS and MES products expose, and `scripts/synthetic-cmms.ts`
// serves it for tests and demos.

export interface ExternalRecord { id: string; version: string; fields: Record<string, unknown> }

export type ApplyResult =
  | { ok: true; record: ExternalRecord }
  | { ok: false; stale: boolean; status: number; detail: string };

export interface SystemAdapter {
  readonly id: string;
  list(): Promise<ExternalRecord[]>;
  read(recordId: string): Promise<ExternalRecord | null>;
  /** `ifVersion` makes the write conditional: the system refuses it if the record moved. */
  apply(recordId: string | null, fields: Record<string, unknown>, ifVersion: string | null, opts?: { compensating?: boolean }): Promise<ApplyResult>;
}

export class CmmsRestAdapter implements SystemAdapter {
  constructor(readonly id: string, private url: string, private token?: string) {}

  private headers(extra: Record<string, string> = {}) {
    return { "content-type": "application/json", ...(this.token ? { authorization: `Bearer ${this.token}` } : {}), ...extra };
  }

  private static toRecord(x: Record<string, unknown>): ExternalRecord {
    const { id, version, ...fields } = x;
    return { id: String(id), version: String(version), fields };
  }

  async list() {
    const r = await fetch(`${this.url}/workorders`, { headers: this.headers(), signal: AbortSignal.timeout(10_000) });
    if (!r.ok) throw new Error(`${this.id}: list HTTP ${r.status}`);
    return ((await r.json()) as Array<Record<string, unknown>>).map(CmmsRestAdapter.toRecord);
  }

  async read(recordId: string) {
    const r = await fetch(`${this.url}/workorders/${encodeURIComponent(recordId)}`, { headers: this.headers(), signal: AbortSignal.timeout(10_000) });
    if (r.status === 404) return null;
    if (!r.ok) throw new Error(`${this.id}: read HTTP ${r.status}`);
    return CmmsRestAdapter.toRecord((await r.json()) as Record<string, unknown>);
  }

  async apply(recordId: string | null, fields: Record<string, unknown>, ifVersion: string | null): Promise<ApplyResult> {
    const r = recordId === null
      ? await fetch(`${this.url}/workorders`, { method: "POST", headers: this.headers(), body: JSON.stringify(fields), signal: AbortSignal.timeout(10_000) })
      : await fetch(`${this.url}/workorders/${encodeURIComponent(recordId)}`, {
          method: "PATCH", headers: this.headers(ifVersion ? { "if-match": ifVersion } : {}), body: JSON.stringify(fields), signal: AbortSignal.timeout(10_000),
        });
    if (r.ok) return { ok: true, record: CmmsRestAdapter.toRecord((await r.json()) as Record<string, unknown>) };
    return { ok: false, stale: r.status === 409 || r.status === 412, status: r.status, detail: (await r.text()).slice(0, 300) };
  }
}
