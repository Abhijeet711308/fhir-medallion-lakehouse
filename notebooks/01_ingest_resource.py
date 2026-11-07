# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Ingest one FHIR resource (Raw + Bronze)
# MAGIC
# MAGIC For the chosen resource this notebook
# MAGIC 1. calls the FHIR API day by day (`_lastUpdated` window) and follows the `next` links,
# MAGIC 2. saves every page untouched in the raw zone (`date=.../run_id=...`),
# MAGIC 3. logs each call in `api_call_log` (url, time called, time saved, file),
# MAGIC 4. loads the pages into the bronze Delta table with `extraction_timestamp` and `api_url_or_params`.
# MAGIC
# MAGIC Leave the dates blank to continue from the saved watermark (first run = last 3 days).

# COMMAND ----------

import os
import sys

SRC = os.path.abspath(os.path.join(os.getcwd(), "..", "src"))
if SRC not in sys.path:
    sys.path.insert(0, SRC)

from datetime import datetime, timezone

from fhir_pipeline.config import load_config
from fhir_pipeline.ingest import run_resource_ingestion

dbutils.widgets.text("resource", "Patient", "FHIR resource")
dbutils.widgets.text("start_date", "", "Start date YYYY-MM-DD (optional)")
dbutils.widgets.text("end_date", "", "End date YYYY-MM-DD, inclusive (optional)")
dbutils.widgets.dropdown("payload_format", "json", ["json", "xml"], "Payload format")
dbutils.widgets.text("run_id", "", "Run id (blank = generate)")
dbutils.widgets.text("config_path", "", "Config file (blank = default)")

resource = dbutils.widgets.get("resource")
run_id = dbutils.widgets.get("run_id") or datetime.now(timezone.utc).strftime("manual_%Y%m%dT%H%M%S")
cfg = load_config(dbutils.widgets.get("config_path") or None)

print(f"resource={resource}  run_id={run_id}")

# COMMAND ----------

result = run_resource_ingestion(
    spark,
    cfg,
    resource=resource,
    run_id=run_id,
    start_date=dbutils.widgets.get("start_date"),
    end_date=dbutils.widgets.get("end_date"),
    payload_format=dbutils.widgets.get("payload_format"),
)
print(result)

# COMMAND ----------

# quick look at what landed in bronze for this run
from fhir_pipeline.config import table_name

display(
    spark.table(table_name(cfg, "bronze", resource))
    .where(f"run_id = '{run_id}'")
    .select("resource_id", "version_id", "last_updated", "extraction_timestamp", "api_url_or_params", "page_number")
    .limit(20)
)
