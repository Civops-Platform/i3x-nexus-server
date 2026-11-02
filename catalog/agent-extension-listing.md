# CESMII Smart Manufacturing Catalog — listing sheet: i3X Nexus Agent Extension

Fields follow what the Catalog shows on every entry (catalog.cesmii.net).

| Field | Value |
|---|---|
| Name | i3X Nexus Agent Extension |
| Vendor | CivOps |
| Vendor website | https://civops.io |
| SM imperative | i3X |
| Category | Developer Resource (specification with a reference implementation) |
| License | Specification © CivOps, offered for comment; reference implementation MIT |
| Asset link | https://github.com/Civops-Platform/i3x-nexus-server/blob/main/docs/i3X-Nexus-Agent-Extension.pdf |
| Builds on | i3X Nexus Profile, contributed by LNS Research: https://github.com/LNS-Research/i3x-nexus-profile |
| Tags | #AIAgents #MCP #Governance #Security #Audit #Specification |
| Hero image | `catalog/agent-extension-1600x900.png` → https://raw.githubusercontent.com/Civops-Platform/i3x-nexus-server/main/catalog/agent-extension-1600x900.png |
| Image source | `catalog/src/agent-extension.html` |
| Vendor logo | https://raw.githubusercontent.com/Civops-Platform/i3x-nexus-server/main/catalog/civops-logo-light.png (light), https://raw.githubusercontent.com/Civops-Platform/i3x-nexus-server/main/catalog/civops-logo-dark.png (dark) |
| Documents | https://raw.githubusercontent.com/Civops-Platform/i3x-nexus-server/main/docs/i3X-Nexus-Agent-Extension.pdf |

## Description (short)

Rules for running many AI agents against an i3X server: each one works for a named person, inside part of the plant, on a budget, and one switch stops them all.

## Description (long)

The i3X Nexus Profile already says an AI agent may ask for a change but never approve one. This extension is what a site needs once it has more than a few agents.

Every agent works under a grant from a named person, which sets one task, the parts of the plant it covers, what it may do there, how many changes it may ask for, and when it stops. Each request an agent raises is recorded with the person it was working for, and that person cannot approve it. One freeze switch stops every agent write on the site while people carry on, and changes already approved for agents wait until it is released. A registry shows each agent's grants, how its requests turned out, and what it was refused.

Agents that speak MCP can use the server directly, through the same checks as any i3X client, and there is no approval tool for them to find. Approvals can be signed over the exact version of the request the approver read. The ledger's latest hash is signed and kept outside the database, so a rewritten history no longer matches.

The reference implementation is the CivOps i3X Nexus Server (MIT). With all of this switched on it still passes the CESMII i3X 1.0 conformance suite (Full 1.0 Compliance, three runs), and its tests cover each rule on synthetic data, including through the standby of a redundant pair.
