// Shared fixtures for the governed-write tests (Levels 3 and 5).

import { sha256, type PrincipalDef } from "../src/core/auth";
import type { SiteConfig } from "../src/model/config";
import { exampleConfig } from "./helpers";

export const AUTH_NS = "https://example.com/i3x/authority/1.0";
export const T = { planner: "t-planner", planner2: "t-planner2", manager: "t-manager", plant: "t-plant", agent: "t-agent", authority: "t-authority", reader: "t-reader" };

export function governedConfig(cmmsUrl: string): SiteConfig {
  const c = exampleConfig();
  c.namespaces.push({ uri: AUTH_NS, name: "Example authority", version: "1.0" }, { uri: "https://example.com/i3x/cmms/1.0", name: "Example CMMS", version: "1.0" });
  c.types.push(
    { id: "AuthorityLimit", name: "Authority limit", namespaceUri: AUTH_NS, kind: "record", write: "W0", priority: "P2",
      attributes: ["site", "exceptionType", "ownerRole", "aboveOwnerRole"].map((name) => ({ name, dataType: "string" as const }))
        .concat([{ name: "approverRoles", dataType: "array" as never }, { name: "boundaryTriggers", dataType: "array" as never },
          { name: "delegatedLimitUsd", dataType: "number" as never }, { name: "materialityThresholdUsd", dataType: "number" as never }]) },
    { id: "WorkOrder", name: "Work order", namespaceUri: "https://example.com/i3x/cmms/1.0", kind: "external", write: "W1",
      attributes: ["asset", "title", "status", "priority", "assignedTo"].map((name) => ({ name, dataType: "string" as const })) },
  );
  c.objects.push({ id: "limit:maintenance-reprioritisation", name: "Maintenance reprioritisation", typeId: "AuthorityLimit", parentId: "site:riverside",
    initial: { site: "example-riverside", exceptionType: "maintenance-reprioritisation", ownerRole: "maintenance-manager", aboveOwnerRole: "plant-manager",
      approverRoles: ["maintenance-planner"], delegatedLimitUsd: 5000, materialityThresholdUsd: 25000, boundaryTriggers: ["safety"] } });
  c.systems = [{ id: "cmms", kind: "cmms-rest", url: cmmsUrl, typeId: "WorkOrder", name: "Synthetic CMMS" }];
  return c;
}

export const P = (id: string, kind: PrincipalDef["kind"], tok: string, roles: string[] = [], owns: string[] = []): PrincipalDef =>
  ({ id, kind, scopes: ["read"], owns, roles, tokenSha256: sha256(tok) });
