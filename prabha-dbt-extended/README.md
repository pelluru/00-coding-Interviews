Root for prabha-dbt multi-target dbt scaffold.


## CI
A GitHub Actions workflow is included at `.github/workflows/dbt-ci.yml`.

- On every push/PR: runs `dbt deps` + `dbt parse` for `databricks`, `postgres`, and `snowflake` subprojects.
- To execute `dbt seed/run/test`, manually dispatch the workflow and set `run_tests=true`, after adding the adapter credentials as repository **Secrets**:
  - **Databricks**: `DBR_HOST`, `DBR_HTTP_PATH`, `DBR_TOKEN`, (optional) `DBR_CATALOG`, `DBR_SCHEMA`
  - **Postgres**: `PG_HOST`, `PG_PORT`, `PG_USER`, `PG_PASSWORD`, `PG_DATABASE`, `PG_SCHEMA`
  - **Snowflake**: `SF_ACCOUNT`, `SF_USER`, `SF_PASSWORD`, `SF_ROLE`, `SF_WAREHOUSE`, `SF_DATABASE`, `SF_SCHEMA`

Each job sets `DBT_PROFILES_DIR` to the subproject folder so dbt uses that `profiles.yml` directly.
