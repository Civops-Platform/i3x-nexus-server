// Writes config/example-governed.json: the example site plus an authority limit and a
// synthetic CMMS behind tier W1. Fictional data only. Run: npx tsx scripts/make-governed-config.ts
import { writeFileSync, readFileSync } from "node:fs";
const c = JSON.parse(readFileSync("config/example-site.json", "utf8"));
const AUTH = "https://example.com/i3x/authority/1.0", CMMS = "https://example.com/i3x/cmms/1.0";
c.site.name = "Example Manufacturing Co. — Riverside, governed (synthetic)";
c.namespaces.push({ uri: AUTH, name: "Example authority", version: "1.0" }, { uri: CMMS, name: "Example CMMS", version: "1.0" });
c.types.push(
  { id: "AuthorityLimit", name: "Authority limit", namespaceUri: AUTH, kind: "record", write: "W0", priority: "P2", attributes: [
    { name: "site", dataType: "string" }, { name: "exceptionType", dataType: "string" }, { name: "ownerRole", dataType: "string" }, { name: "aboveOwnerRole", dataType: "string" },
    { name: "approverRoles", dataType: "array" }, { name: "boundaryTriggers", dataType: "array" },
    { name: "delegatedLimitUsd", dataType: "number" }, { name: "materialityThresholdUsd", dataType: "number" } ] },
  { id: "WorkOrder", name: "Work order", namespaceUri: CMMS, kind: "external", write: "W1", priority: "P2",
    attributes: ["asset", "title", "status", "priority", "assignedTo"].map((name) => ({ name, dataType: "string" })) });
c.objects.push({ id: "limit:maintenance-reprioritisation", name: "Maintenance reprioritisation", typeId: "AuthorityLimit", parentId: "site:riverside",
  initial: { site: "example-riverside", exceptionType: "maintenance-reprioritisation", ownerRole: "maintenance-manager", aboveOwnerRole: "plant-manager",
    approverRoles: ["maintenance-planner"], delegatedLimitUsd: 5000, materialityThresholdUsd: 25000, boundaryTriggers: ["safety"] } });
c.systems = [{ id: "cmms", kind: "cmms-rest", url: "http://127.0.0.1:8430", typeId: "WorkOrder", name: "Synthetic CMMS" }];
writeFileSync("config/example-governed.json", JSON.stringify(c, null, 2) + "\n");
