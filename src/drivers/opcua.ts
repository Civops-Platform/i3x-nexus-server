// OPC UA source: one session, one subscription, one monitored item per bound
// attribute. The address is a NodeId string (`ns=2;s=Line2.Filler.Speed`).
//
// Values keep the server's SourceTimestamp. StatusCode severity maps to quality
// (Good / Uncertain / Bad). A lost session marks every attribute Bad; node-opcua's
// own reconnect brings it back and the monitored items resume.
//
// Read-only. The OPC UA Write service is the W2 conduit of phase 4 and is not
// wired here on purpose.

import {
  AttributeIds, DataType, StatusCodes, Variant, ClientMonitoredItem, MessageSecurityMode, OPCUAClient, SecurityPolicy, TimestampsToReturn,
  type ClientSession, type ClientSubscription, type DataValue,
} from "node-opcua-client";
import type { Binding, SourceDef } from "../model/config";
import type { Driver, DriverSink } from "./driver";
import { qualityOfStatus } from "./driver";

export class OpcUaDriver implements Driver {
  private client: OPCUAClient | null = null;
  private session: ClientSession | null = null;
  private sub: ClientSubscription | null = null;
  private connected = false;
  private everConnected = false;
  private detail = "not started";
  constructor(readonly source: SourceDef) {}

  /**
   * Returns at once: the session is built in the background, so a source that is
   * down at start-up delays nothing else. Until it connects its attributes read Bad.
   */
  async start(points: Array<{ attributeId: string; binding: Binding }>, sink: DriverSink) {
    this.detail = "connecting";
    void this.run(points, sink);
    setTimeout(() => { if (!this.everConnected) sink.lost(`no session with ${this.source.endpoint} after 5 s`); }, 5000).unref();
  }

  private async run(points: Array<{ attributeId: string; binding: Binding }>, sink: DriverSink) {
    const o = (this.source.options ?? {}) as { securityMode?: string; securityPolicy?: string; publishingIntervalMs?: number };
    this.client = OPCUAClient.create({
      applicationName: "i3x-nexus",
      endpointMustExist: false,
      keepSessionAlive: true,
      connectionStrategy: { initialDelay: 1000, maxDelay: 10_000, maxRetry: Number.MAX_SAFE_INTEGER },
      securityMode: (MessageSecurityMode as unknown as Record<string, MessageSecurityMode>)[o.securityMode ?? "None"],
      securityPolicy: (SecurityPolicy as unknown as Record<string, SecurityPolicy>)[o.securityPolicy ?? "None"],
    });
    const down = (why: string) => {
      if (this.connected) sink.lost(why);
      this.connected = false;
      this.detail = why;
    };
    // A server that shuts down cleanly does not always raise connection_lost; any of
    // these means the session is no longer delivering values.
    this.client.on("connection_lost", () => down("OPC UA connection lost"));
    this.client.on("start_reconnection", () => down("OPC UA reconnecting"));
    this.client.on("backoff", () => down("OPC UA server unreachable, retrying"));
    this.client.on("close", () => down("OPC UA connection closed"));
    this.client.on("connection_reestablished", () => { this.connected = true; this.detail = "connected"; sink.restored(); });

    try {
      await this.client.connect(this.source.endpoint!);
      this.session = await this.client.createSession();
    } catch (e) {
      if (!this.client) return; // stopped while connecting
      sink.lost(`cannot connect to ${this.source.endpoint}: ${(e as Error).message}`);
      this.detail = (e as Error).message;
      return;
    }
    if (!this.client) return;
    this.session.on("keepalive_failure", () => down("OPC UA session keep-alive failed"));
    this.session.on("session_closed", () => down("OPC UA session closed"));
    this.connected = true;
    this.everConnected = true;
    this.detail = "connected";
    const interval = o.publishingIntervalMs ?? 250;
    this.sub = await this.session.createSubscription2({
      requestedPublishingInterval: interval, requestedLifetimeCount: 600, requestedMaxKeepAliveCount: 20,
      maxNotificationsPerPublish: 0, publishingEnabled: true, priority: 10,
    });
    for (const { attributeId, binding } of points) {
      const item = ClientMonitoredItem.create(this.sub,
        { nodeId: binding.address, attributeId: AttributeIds.Value },
        { samplingInterval: binding.scanMs ?? interval, discardOldest: true, queueSize: 10 },
        TimestampsToReturn.Both);
      item.on("changed", (dv: DataValue) => {
        const ts = (dv.sourceTimestamp ?? dv.serverTimestamp ?? new Date()).toISOString();
        const raw = dv.value?.value;
        const value = typeof raw === "bigint" ? Number(raw) : raw ?? null;
        sink.value(attributeId, value, qualityOfStatus(dv.statusCode.value), ts);
      });
      // A node that cannot be monitored (wrong NodeId, no access) is Bad, not absent.
      item.on("err", () => sink.value(attributeId, null, "Bad", new Date().toISOString()));
    }
  }

  async stop() {
    const c = this.client;
    this.client = null;
    const quick = <T>(p: Promise<T> | undefined) => Promise.race([p, new Promise((r) => setTimeout(r, 2000))]);
    try { await quick(this.sub?.terminate()); } catch { /* already gone */ }
    try { await quick(this.session?.close()); } catch { /* already gone */ }
    try { await quick(c?.disconnect()); } catch { /* already gone */ }
    this.sub = null; this.session = null;
  }

  status() { return { connected: this.connected, detail: this.detail }; }

  /**
   * OPC UA Write of the Value attribute. The variant type is taken from the node's
   * current value, so a Double stays a Double and an Int32 stays an Int32. A bad
   * status from the server is the device refusing; no session is a transient failure.
   */
  async write(address: string, value: unknown) {
    const s = this.session;
    if (!s || !this.connected) return { ok: false as const, detail: "no OPC UA session" };
    try {
      const cur = await s.read({ nodeId: address, attributeId: AttributeIds.Value });
      const dataType = cur.value?.dataType ?? DataType.Double;
      const sc = await s.write({ nodeId: address, attributeId: AttributeIds.Value, value: { value: new Variant({ dataType, value }) } });
      if (sc.value === StatusCodes.Good.value) return { ok: true as const };
      return { ok: false as const, detail: `OPC UA write refused: ${sc.name}`, permanent: true };
    } catch (e) {
      return { ok: false as const, detail: `OPC UA write failed: ${(e as Error).message}` };
    }
  }
}
