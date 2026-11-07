# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Silver for one resource
# MAGIC Reads the bronze rows of a run, parses the JSON into typed columns, cleans and de-duplicates them,
# MAGIC then applies SCD Type 2 into the silver table (history kept with `valid_from`, `valid_to`, `is_current`).

# COMMAND ----------

import os
import sys

SRC = os.path.abspath(os.path.join(os.getcwd(), "..", "src"))
if SRC not in sys.path:
    sys.path.insert(0, SRC)

import traceback

from fhir_pipeline.audit import log_run
from fhir_pipeline.config import load_config, table_name
from fhir_pipeline.fhir_client import utc_now
from fhir_pipeline.silver import load_silver

dbutils.widgets.text("resource", "Patient", "FHIR resource")
dbutils.widgets.text("run_id", "", "Run id of the bronze load (required)")
dbutils.widgets.text("config_path", "", "Config file (blank = default)")

resource = dbutils.widgets.get("resource")
run_id = dbutils.widgets.get("run_id")
assert run_id, "run_id is required, use the same value as the ingest task"
cfg = load_config(dbutils.widgets.get("config_path") or None)

# COMMAND ----------

started = utc_now()
try:
    stats = load_silver(spark, cfg, resource, run_id)
    msg = (
        f"new keys={stats['new_keys']}, new versions={stats['new_versions']}, "
        f"unchanged={stats['unchanged']}"
    )
    log_run(spark, cfg, run_id, resource, "silver", "SUCCESS", started, stats["rows_read"], stats["inserted_rows"], msg)
    print(stats)
except Exception as exc:
    log_run(spark, cfg, run_id, resource, "silver", "FAILED", started, None, None, f"{exc}\n{traceback.format_exc()[-800:]}")
    raise

# COMMAND ----------

target = table_name(cfg, "silver", resource)
display(spark.sql(f"SELECT is_current, COUNT(*) AS rows FROM {target} GROUP BY is_current"))
