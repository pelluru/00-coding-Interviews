# Databricks Word Count Variations (One Notebook Per Variation)

This bundle contains **7 Databricks notebooks** demonstrating different **PySpark DataFrame** word count variations, plus sample data.

## Contents
- `notebooks/01_basic_word_count.py`
- `notebooks/02_normalize_filter.py`
- `notebooks/03_regex_tokenizer.py`
- `notebooks/04_sql_tempview.py`
- `notebooks/05_wordcount_by_category.py`
- `notebooks/06_pivot_wordcount.py`
- `notebooks/07_weighted_wordcount.py`
- `data/sample_text.txt`
- `data/category_text.csv`

## Quick Start (Databricks)

1. Import these notebooks into Databricks (Workspace → Import → Upload files).
2. Upload `data/*` to DBFS, e.g. to `/FileStore/wordcount/`:
   - In a Databricks notebook cell:
     ```python
     dbutils.fs.mkdirs("dbfs:/FileStore/wordcount")
     dbutils.fs.put("dbfs:/FileStore/wordcount/sample_text.txt", open("/Workspace/Repos_or_upload_path/data/sample_text.txt").read(), True)
     dbutils.fs.put("dbfs:/FileStore/wordcount/category_text.csv", open("/Workspace/Repos_or_upload_path/data/category_text.csv").read(), True)
     ```
   - Or use the **Data** UI → DBFS → Upload.
3. In each notebook, update `DATA_BASE` if needed (defaults to `/FileStore/wordcount`).

> Tip: You can also mount cloud storage and point `DATA_BASE` there.

## Spark Version
These notebooks target Spark 3.x / Databricks Runtime 10+ (should also work on later runtimes).

