# Databricks Cost Playbook — DLT + Auto Loader + UC

## Compute & Sizing
- Use **autoscaling** with small min nodes for streaming; scale up on backlogs.
- Prefer **Photon** runtimes for SQL/DLT where supported.
- Separate **ingest** (Auto Loader) from **transform** (DLT) clusters to match workloads.

## Storage & Tables
- **Delta OPTIMIZE** with Z-ORDER on common predicates (e.g., user_id, event_time).
- **VACUUM** with retention aligned to compliance; avoid too-aggressive retention.
- Use **checkpoint** and **schema locations** on cost-effective storage classes with lifecycle policies.

## Scheduling & Refresh
- Use **continuous** for near-real-time DLT; **triggered** or **batch windows** for cost-efficient SIEM/BI.
- For MVs, choose refresh cadence based on SLAs; avoid excessive auto-refresh.

## Data Quality & Reprocessing
- Fail-fast with **EXPECT** + quarantine tables; replay using **time travel** or **backfill windows**.
- Hash-based change detection to reduce MERGE costs in gold layers.

## Governance & Caching
- Grant **SELECT** only to analysts; restrict MODIFY/CREATE to engineering roles.
- Leverage **SQL Warehouses** with **auto-stop** and **query result caching**.
- Tag high-cost jobs and monitor with usage dashboards / billable metrics.
