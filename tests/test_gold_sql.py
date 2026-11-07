"""Runs the gold view SQL against small in-memory tables to catch typos and bad joins."""

import pytest

pytest.importorskip("pyspark")

from pyspark.sql import functions as F

from fhir_pipeline.config import load_config
from fhir_pipeline.gold import render_statements
from fhir_pipeline.scd2 import add_row_hash
from fhir_pipeline.silver import flatten_bronze
from test_transformations import CONDITION, ENCOUNTER, OBSERVATION, PATIENT, bronze_df

CFG = load_config(overrides={"storage": {"catalog": "spark_catalog"}})


def _write_silver(spark, resource, payload, name):
    df = flatten_bronze(bronze_df(spark, payload), resource)
    df = (
        df.withColumn("valid_from", F.col("effective_from"))
        .withColumn("valid_to", F.lit(None).cast("timestamp"))
        .withColumn("is_current", F.lit(True))
        .withColumn("version_number", F.lit(1))
    )
    df.write.mode("overwrite").saveAsTable(f"spark_catalog.fhir_silver.{name}")


def test_gold_views_build_and_return_rows(spark):
    for schema in ("fhir_silver", "fhir_gold", "fhir_meta"):
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {schema}")

    _write_silver(spark, "Patient", PATIENT, "patient")
    _write_silver(spark, "Encounter", ENCOUNTER, "encounter")
    _write_silver(spark, "Observation", OBSERVATION, "observation")
    _write_silver(spark, "Condition", CONDITION, "condition")

    spark.sql(
        "CREATE TABLE IF NOT EXISTS fhir_meta.pipeline_run_log (run_id string, resource_type string, layer string, status string, "
        "rows_read long, rows_written long, started_at timestamp, finished_at timestamp, message string) USING parquet"
    )
    spark.sql(
        "CREATE TABLE IF NOT EXISTS fhir_meta.api_call_log (run_id string, resource_type string, window_start timestamp, "
        "entry_count int, http_status int, called_at timestamp, saved_at timestamp) USING parquet"
    )

    for stmt in render_statements(CFG):
        spark.sql(stmt)

    summary = spark.sql("SELECT * FROM spark_catalog.fhir_gold.vw_patient_summary").collect()
    assert len(summary) == 1
    row = summary[0]
    assert (row["encounter_count"], row["observation_count"], row["condition_count"]) == (1, 1, 1)

    enc = spark.sql("SELECT * FROM spark_catalog.fhir_gold.fact_encounter").collect()[0]
    assert enc["patient_in_warehouse"] is True and enc["duration_minutes"] == 30.0

    vitals = spark.sql("SELECT * FROM spark_catalog.fhir_gold.vw_vital_signs_summary").collect()
    assert vitals[0]["avg_value"] == 72.0

    daily = spark.sql("SELECT * FROM spark_catalog.fhir_gold.vw_daily_encounters").collect()
    assert daily[0]["encounter_count"] == 1
