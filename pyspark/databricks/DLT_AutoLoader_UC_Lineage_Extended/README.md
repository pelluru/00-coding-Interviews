# End-to-End Databricks Pack — DLT + Auto Loader + Unity Catalog Lineage

**What you get**
- **DLT pipeline** (SQL): bronze → silver → gold with `EXPECT` quality rules
- **Auto Loader** (Python): ingest raw JSON/CSV from cloud storage with schema evolution
- **Unity Catalog (UC)** SQL: catalog/schema setup, grants, lineage discovery
- **Deployment**: pipeline JSON (workflows-ready), sample CI steps
- **Interview Q&A** (senior/architect level)

---

## Quick Start
1. Create a UC **catalog** and **schema** (run `uc_setup.sql`).
2. Upload raw files to your storage path (e.g., `/mnt/raw/transactions/`).
3. Run Auto Loader notebook (`autoloader_ingest.py`) to land into `raw.transactions_raw` (Delta).
4. Create/run DLT pipeline using `dlt_pipeline.sql` and `dlt_pipeline_settings.json`.
5. (Optional) Create **materialized view** for BI (`mv_gold.sql`).
6. Inspect **lineage** via UC system tables (`uc_lineage.sql`).

---
