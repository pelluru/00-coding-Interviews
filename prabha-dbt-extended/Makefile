
.DEFAULT_GOAL := help

help: ## Show this help
    @grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

venv: ## Create virtual env
    python -m venv .venv && . .venv/bin/activate && pip install -r requirements.txt && pre-commit install

lint: ## Run sqlfluff lint
    sqlfluff lint

fix: ## Run sqlfluff fix
    sqlfluff fix

parse-%: ## dbt parse in subproject (e.g., make parse-postgres)
    cd $* && DBT_PROFILES_DIR=$$(pwd) dbt deps && DBT_PROFILES_DIR=$$(pwd) dbt parse

ci-run-%: ## dbt seed/run/test in subproject (requires configured target)
    cd $* && DBT_PROFILES_DIR=$$(pwd) dbt deps && DBT_PROFILES_DIR=$$(pwd) dbt seed --full-refresh && DBT_PROFILES_DIR=$$(pwd) dbt run && DBT_PROFILES_DIR=$$(pwd) dbt test
