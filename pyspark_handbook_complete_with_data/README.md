# PySpark Handbook — Complete Bundle

This repository-style bundle includes **250 complex PySpark questions** (PDF + code), **topic PDFs**, **customer I/O formats pack**, and **Databricks notebooks**.

## Structure
- `pdfs/`
  - `PySpark_250_All.pdf` — full handbook
  - Topic PDFs: Streaming, Performance, Delta_CDC, Windows
  - `PySpark_Top50.pdf` — focused set
  - `customer_formats_pack.pdf` — JSON/CSV/Parquet/XML examples
- `code/`
  - `250_questions/` — one `.py` per question with comments (pure PySpark)
  - `customer_formats/` — format-specific examples (10 scripts)
- `notebooks/` — Databricks Source-form notebooks (`.py` with `# COMMAND ----------` and `%md`)
  - `index.py`, `streaming.py`, `performance.py`, `delta_cdc.py`, `windows.py`
  - `customer_formats_notebook.py`
- `scripts/`
  - `databricks_import_and_export.sh` — import Source notebooks via CLI and export a real `.dbc`

## Quick Start
1. Import notebooks (ZIP or individual `.py`) into Databricks → Workspace → Import → File → **Source**.
2. Open `index.py` (or `driver.py` if using the Repos bundle variant) and navigate topics.
3. Use the `.py` files in `code/` for unit testing (pytest + chispa) or to embed in jobs.

## Notes
- Some examples demonstrate Delta MERGE patterns; ensure Delta Lake is available or port to SQL/DataFrame ops.
- Replace placeholder paths and DataFrames (e.g., `df`, `fact`, `dim`, `streaming_df`) per your environment.