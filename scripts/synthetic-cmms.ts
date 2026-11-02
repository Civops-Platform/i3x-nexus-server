// A synthetic CMMS for tests and demos: the REST work-order shape `cmms-rest` speaks.
// In memory, versioned, honours If-Match. Fictional data only.
//
//   npx tsx scripts/synthetic-cmms.ts [port] [host]     (default 8430 on 127.0.0.1)

import http from "node:http";
import type { AddressInfo } from "node:net";

export interface WorkOrder { id: string; version: number; [k: string]: unknown }

export interface CmmsOptions {
  /** Test hooks. */
  ignoreWrites?: () => boolean;
  failNext?: () => boolean;
}

export function startSyntheticCmms(port = 0, o: CmmsOptions = {}, host = "127.0.0.1") {
  const orders = new Map<string, WorkOrder>([
    ["WO-1001", { id: "WO-1001", version: 1, asset: "Line 2 filler", title: "Replace capping head bearing", status: "open", priority: "medium", assignedTo: null }],
    ["WO-1002", { id: "WO-1002", version: 1, asset: "Line 2 labeller", title: "Inspect glue station", status: "open", priority: "low", assignedTo: "crew-b" }],
  ]);
  let next = 1003;
  const server = http.createServer(async (req, res) => {
    const send = (s: number, b: unknown) => { res.writeHead(s, { "content-type": "application/json" }); res.end(JSON.stringify(b)); };
    const chunks: Buffer[] = [];
    for await (const c of req) chunks.push(c as Buffer);
    const body = chunks.length ? JSON.parse(Buffer.concat(chunks).toString()) : {};
    const m = /^\/workorders(?:\/([^/]+))?$/.exec(req.url ?? "");
    if (!m) return send(404, { error: "not found" });
    const id = m[1] ? decodeURIComponent(m[1]) : null;
    if (req.method === "GET" && !id) return send(200, [...orders.values()]);
    if (req.method === "GET" && id) return orders.has(id) ? send(200, orders.get(id)) : send(404, { error: "no such work order" });
    if (o.failNext?.()) return send(500, { error: "injected failure" });
    if (req.method === "POST" && !id) {
      const wo: WorkOrder = { status: "open", ...body, id: `WO-${next++}`, version: 1 };
      orders.set(wo.id, wo);
      return send(201, wo);
    }
    if (req.method === "PATCH" && id) {
      const cur = orders.get(id);
      if (!cur) return send(404, { error: "no such work order" });
      const ifm = req.headers["if-match"];
      if (ifm && String(cur.version) !== ifm) return send(409, { error: `version is ${cur.version}, not ${ifm}` });
      if (o.ignoreWrites?.()) return send(200, cur); // acknowledges, changes nothing (read-back must catch it)
      const { id: _i, version: _v, ...fields } = body;
      const upd = { ...cur, ...fields, version: cur.version + 1 };
      orders.set(id, upd);
      return send(200, upd);
    }
    return send(405, { error: "method not allowed" });
  });
  return new Promise<{ url: string; close: () => Promise<void>; orders: Map<string, WorkOrder> }>((r) => server.listen(port, host, () => {
    const p = (server.address() as AddressInfo).port;
    r({ url: `http://${host}:${p}`, orders, close: () => new Promise((x) => { server.closeAllConnections(); server.close(() => x()); }) });
  }));
}

if (process.argv[1]?.endsWith("synthetic-cmms.ts")) {
  const s = await startSyntheticCmms(Number(process.argv[2] ?? 8430), {}, process.argv[3] ?? "127.0.0.1");
  console.log(`synthetic CMMS on ${s.url}/workorders`);
}
