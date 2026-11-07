# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - Gold
# MAGIC Creates the reporting views (see `sql/gold_views.sql`) and the `dim_date` table.
# MAGIC Views only read the current rows from silver, so they always show the latest version of each record.

# COMMAND ----------

import os
import sys

SRC = os.path.abspath(os.path.join(os.getcwd(), "..", "src"))
if SRC not in sys.path:
    sys.path.insert(0, SRC)

from fhir_pipeline.audit import log_run
from fhir_pipeline.config import load_config
from fhir_pipeline.fhir_client import utc_now
from fhir_pipeline.gold import build_gold

dbutils.widgets.text("run_id", "", "Run id")
dbutils.widgets.text("config_path", "", "Config file (blank = default)")
run_id = dbutils.widgets.get("run_id") or "manual"
cfg = load_config(dbutils.widgets.get("config_path") or None)

# COMMAND ----------

started = utc_now()
result = build_gold(spark, cfg)
log_run(spark, cfg, run_id, "ALL", "gold", "SUCCESS", started, None, result["dim_date_rows"], str(result))
print(result)

# COMMAND ----------

gold = f"{cfg['storage']['catalog']}.{cfg['storage']['schemas']['gold']}"
display(spark.sql(f"SELECT * FROM {gold}.vw_patient_summary ORDER BY encounter_count DESC LIMIT 20"))
