# CI Outline for DLT + UC

- Validate SQL (sqlfluff or dbt parser if using dbt for some models).
- Validate JSON (`dlt_pipeline_settings.json`) schema.
- Dry-run DLT parse via Databricks workflows API (optional).
- Smoke test permissions: `SHOW GRANTS` against catalog/schema.
- (Optional) Run a small sample Auto Loader stream in a dev environment using a stub dataset.
