# CESMII Smart Manufacturing Catalog — listing sheet

Fields follow what the Catalog shows on every entry (catalog.cesmii.net): name, vendor, description,
tags, SM imperative, category, asset link, vendor website, images. Image URLs below assume the repo is
public at the asset link; Jonathan links them from there.

| Field | Value |
|---|---|
| Name | i3X Nexus Server |
| Vendor | CivOps |
| Vendor website | https://civops.io |
| SM imperative | i3X |
| Category | Open Source |
| License | MIT |
| Asset link | https://github.com/Civops-Platform/i3x-nexus-server |
| Tags | #Server #Governance #Redundancy #AIAgents #OPCUA #MQTT #OpenSource |
| Hero image | `catalog/hero-1600x900.png` → https://raw.githubusercontent.com/Civops-Platform/i3x-nexus-server/main/catalog/hero-1600x900.png |
| Image source | `catalog/src/hero.html` (rendered by `catalog/render.sh`, headless Chrome at 1600×900) |
| Vendor logo | `catalog/civops-logo-light.png` (light backgrounds), `catalog/civops-logo-dark.png` (dark), 800×192; SVG sources in `catalog/src/` |

## Description (short — the line under the title)

One i3X 1.0 endpoint per plant for live values, history, records and media, where every write by a person, a system or an AI agent goes through a governed path, and two nodes keep it up when one of them dies.

## Description (long)

This is CivOps' server for the i3X Nexus Profile that LNS Research published. A plant usually has its live data in OPC UA or MQTT, its history in a historian, its work orders in a CMMS and its camera footage somewhere else again, and every new application has to learn all four. The server puts them behind one i3X address space, so a client that speaks i3X 1.0 sees one plant.

Most of the care went into writes. A record write is a new version in a hash-chained ledger, so a change can be traced back to who made it. A write into a system of record (a work order, say) is a request that someone with the right authority approves, never the person or agent who asked for it, and it is applied only if the record has not moved since, then read back, and undone on its own if the read-back does not match. Setpoints have a hard envelope of range and step size, are never clamped to fit, and need a named person's approval every time.

It runs as a pair over shared Postgres. Either node serves reads and subscriptions, one holds the write lease, and when that node is killed the other takes over. In 20 forced kills under write load, writes came back within 12.5 seconds every time, with 111 writes acknowledged, none lost and none applied twice.

Measured on this code with synthetic data: CESMII's i3X 1.0 conformance suite gives Full 1.0 Compliance with 0 failures (0 warnings over HTTPS), and it still passes when the node holding the write lease is killed in the middle of the run. The test suite covers OPC UA, MQTT and Sparkplug B drivers against real servers, the cluster on real Postgres, and the media gateway against a real MediaMTX.

TypeScript on Node 22, MIT licensed. The Sparkplug connector, the historian adapter, the media bridge, the enterprise mirror, the governed-write kit and the failover harness are also published as packages of their own, for anyone who wants one piece without the server.
