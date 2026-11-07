# Databricks notebook source
# MAGIC %md
# MAGIC # 04 - Validation checks
# MAGIC Run after the pipeline. Shows row counts per layer, the API call log, SCD2 sanity checks and
# MAGIC a sample of records that changed between loads.

# COMMAND ----------

import os
import sys

SRC = os.path.abspath(os.path.join(os.getcwd(), "..", "src"))
if SRC not in sys.path:
    sys.path.insert(0, SRC)

from fhir_pipeline.config import load_config, meta_table, table_name
from fhir_pipeline.silver_models import MODELS

dbutils.widgets.text("config_path", "", "Config file (blank = default)")
cfg = load_config(dbutils.widgets.get("config_path") or None)
resources = cfg["fhir"]["resources"]

# COMMAND ----------

# MAGIC %md
# MAGIC ### Row counts per layer

# COMMAND ----------

rows = []
for r in resources:
    bronze = spark.table(table_name(cfg, "bronze", r))
    silver = spark.table(table_name(cfg, "silver", r))
    rows.append(
        (
            r,
            bronze.count(),
            silver.count(),
            silver.where("is_current").count(),
            silver.where("NOT is_current").count(),
        )
    )
display(spark.createDataFrame(rows, "resource string, bronze_rows long, silver_rows long, silver_current long, silver_history long"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### API calls per run and day

# COMMAND ----------

display(
    spark.sql(
        f"""
        SELECT resource_type, CAST(window_start AS DATE) AS load_date, COUNT(*) AS pages,
               SUM(entry_count) AS resources, MIN(called_at) AS first_call, MAX(saved_at) AS last_saved
        FROM {meta_table(cfg, 'api_call_log')}
        WHERE http_status = 200
        GROUP BY resource_type, CAST(window_start AS DATE)
        ORDER BY resource_type, load_date
        """
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### SCD2 checks (every query should return zero rows)

# COMMAND ----------

KEYS = {r: MODELS[r]["key"] for r in resources}

for r in resources:
    t = table_name(cfg, "silver", r)
    k = KEYS[r]
    more_than_one_current = spark.sql(f"SELECT {k}, COUNT(*) c FROM {t} WHERE is_current GROUP BY {k} HAVING c > 1").count()
    bad_ranges = spark.sql(f"SELECT {k} FROM {t} WHERE valid_to IS NOT NULL AND valid_to < valid_from").count()
    open_but_not_current = spark.sql(f"SELECT {k} FROM {t} WHERE valid_to IS NULL AND NOT is_current").count()
    print(f"{r:12s} keys with >1 current row: {more_than_one_current} | bad date ranges: {bad_ranges} | open history rows: {open_but_not_current}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Records that changed between loads (history rows)

# COMMAND ----------

for r in resources:
    t = table_name(cfg, "silver", r)
    k = KEYS[r]
    print(r)
    display(spark.sql(f"SELECT {k}, version_number, valid_from, valid_to, is_current FROM {t} WHERE {k} IN (SELECT {k} FROM {t} WHERE NOT is_current) ORDER BY {k}, version_number LIMIT 20"))
