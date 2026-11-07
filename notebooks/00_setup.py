# Databricks notebook source
# MAGIC %md
# MAGIC # 00 - Setup
# MAGIC Creates the schemas, the raw-zone volume and the metadata tables.
# MAGIC Safe to run more than once.

# COMMAND ----------

import os
import sys

# the repo is a Databricks Git folder, so src/ sits next to the notebooks folder
SRC = os.path.abspath(os.path.join(os.getcwd(), "..", "src"))
if SRC not in sys.path:
    sys.path.insert(0, SRC)

from fhir_pipeline.bootstrap import ensure_storage
from fhir_pipeline.config import load_config

dbutils.widgets.text("config_path", "", "Config file (blank = default)")
cfg = load_config(dbutils.widgets.get("config_path") or None)

# COMMAND ----------

created = ensure_storage(spark, cfg)
for name, value in created.items():
    print(f"{name:8s} -> {value}")
