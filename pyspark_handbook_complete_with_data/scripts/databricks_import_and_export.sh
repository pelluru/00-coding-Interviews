#!/usr/bin/env bash
# Databricks CLI helper for importing these notebooks and exporting a DBC archive.

set -euo pipefail

# EDIT THESE:
WORKSPACE_DIR="/Shared/pyspark-handbook"   # Workspace folder to import to
LOCAL_NOTEBOOKS_DIR="$(dirname "$0")/../notebooks"
EXPORT_DIR="$(pwd)/export_dbc"

echo "[INFO] Importing notebooks from ${LOCAL_NOTEBOOKS_DIR} to ${WORKSPACE_DIR} (as SOURCE)"
# Create workspace dir if missing
databricks workspace mkdirs "${WORKSPACE_DIR}"

# Import each notebook as SOURCE
for nb in "${LOCAL_NOTEBOOKS_DIR}"/*.py; do
  base="$(basename "$nb")"
  echo "[INFO] Importing ${base}"
  databricks workspace import --format SOURCE "$nb" "${WORKSPACE_DIR}/${base%.py}"
done

echo "[INFO] Exporting to DBC into ${EXPORT_DIR}"
mkdir -p "${EXPORT_DIR}"
databricks workspace export_dir --format DBC "${WORKSPACE_DIR}" "${EXPORT_DIR}"

echo "[DONE] DBC files available in ${EXPORT_DIR}"