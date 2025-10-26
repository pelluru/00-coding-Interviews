# dbt_gold (Databricks)

## Quick Start
```
python -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt
export DBR_HOST=... DBR_HTTP_PATH=... DBR_TOKEN=... DBR_CATALOG=analytics DBR_SCHEMA=marts
dbt deps && DBT_PROFILES_DIR=$(pwd) dbt parse
DBT_PROFILES_DIR=$(pwd) dbt run
DBT_PROFILES_DIR=$(pwd) dbt test
DBT_PROFILES_DIR=$(pwd) dbt source freshness
```
