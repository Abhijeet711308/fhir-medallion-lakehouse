"""Metadata tables: API call log, pipeline run log and per-resource watermark."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from .config import meta_table

API_CALL_SCHEMA = StructType(
    [
        StructField("run_id", StringType()),
        StructField("resource_type", StringType()),
        StructField("payload_format", StringType()),
        StructField("window_start", TimestampType()),
        StructField("window_end", TimestampType()),
        StructField("page_number", IntegerType()),
        StructField("request_url", StringType()),
        StructField("http_status", IntegerType()),
        StructField("attempts", IntegerType()),
        StructField("entry_count", IntegerType()),
        StructField("bytes", LongType()),
        StructField("file_path", StringType()),
        StructField("called_at", TimestampType()),
        StructField("saved_at", TimestampType()),
        StructField("error_message", StringType()),
    ]
)

RUN_LOG_SCHEMA = StructType(
    [
        StructField("run_id", StringType()),
        StructField("resource_type", StringType()),
        StructField("layer", StringType()),
        StructField("status", StringType()),
        StructField("rows_read", LongType()),
        StructField("rows_written", LongType()),
        StructField("started_at", TimestampType()),
        StructField("finished_at", TimestampType()),
        StructField("message", StringType()),
    ]
)

WATERMARK_SCHEMA = StructType(
    [
        StructField("resource_type", StringType()),
        StructField("watermark_ts", TimestampType()),
        StructField("last_run_id", StringType()),
        StructField("updated_at", TimestampType()),
    ]
)


def _create_if_missing(spark: SparkSession, name: str, schema: StructType) -> None:
    ddl = ", ".join(f"`{f.name}` {f.dataType.simpleString()}" for f in schema.fields)
    spark.sql(f"CREATE TABLE IF NOT EXISTS {name} ({ddl}) USING DELTA")


def ensure_meta_tables(spark: SparkSession, cfg: Dict[str, Any]) -> None:
    _create_if_missing(spark, meta_table(cfg, "api_call_log"), API_CALL_SCHEMA)
    _create_if_missing(spark, meta_table(cfg, "run_log"), RUN_LOG_SCHEMA)
    _create_if_missing(spark, meta_table(cfg, "watermark"), WATERMARK_SCHEMA)


# ------------------------------------------------------------------- api calls
def log_api_calls(spark: SparkSession, cfg: Dict[str, Any], rows: List[Dict[str, Any]]) -> None:
    if not rows:
        return
    data = [tuple(r.get(f.name) for f in API_CALL_SCHEMA.fields) for r in rows]
    df = spark.createDataFrame(data, schema=API_CALL_SCHEMA)
    df.write.format("delta").mode("append").saveAsTable(meta_table(cfg, "api_call_log"))


def successful_files(spark: SparkSession, cfg: Dict[str, Any], run_id: str, resource: str) -> "Any":
    """Call-log rows (one per saved page) for this run, used to find the raw files and their metadata."""
    return (
        spark.table(meta_table(cfg, "api_call_log"))
        .where((F.col("run_id") == run_id) & (F.col("resource_type") == resource))
        .where((F.col("http_status") == 200) & F.col("file_path").isNotNull())
    )


# ------------------------------------------------------------------- run log
def log_run(
    spark: SparkSession,
    cfg: Dict[str, Any],
    run_id: str,
    resource: str,
    layer: str,
    status: str,
    started_at: datetime,
    rows_read: Optional[int] = None,
    rows_written: Optional[int] = None,
    message: Optional[str] = None,
) -> None:
    row = (
        run_id,
        resource,
        layer,
        status,
        rows_read,
        rows_written,
        started_at,
        datetime.now(timezone.utc),
        (message or "")[:2000],
    )
    spark.createDataFrame([row], schema=RUN_LOG_SCHEMA).write.format("delta").mode("append").saveAsTable(
        meta_table(cfg, "run_log")
    )


# ------------------------------------------------------------------- watermark
def get_watermark(spark: SparkSession, cfg: Dict[str, Any], resource: str) -> Optional[datetime]:
    rows = (
        spark.table(meta_table(cfg, "watermark"))
        .where(F.col("resource_type") == resource)
        .select("watermark_ts")
        .collect()
    )
    if not rows or rows[0][0] is None:
        return None
    ts = rows[0][0]
    return ts if ts.tzinfo else ts.replace(tzinfo=timezone.utc)


def set_watermark(spark: SparkSession, cfg: Dict[str, Any], resource: str, ts: datetime, run_id: str) -> None:
    table = meta_table(cfg, "watermark")
    now = datetime.now(timezone.utc)
    src = spark.createDataFrame([(resource, ts, run_id, now)], schema=WATERMARK_SCHEMA)
    src.createOrReplaceTempView("_wm_src")
    spark.sql(
        f"""
        MERGE INTO {table} t
        USING _wm_src s
        ON t.resource_type = s.resource_type
        WHEN MATCHED THEN UPDATE SET t.watermark_ts = s.watermark_ts, t.last_run_id = s.last_run_id, t.updated_at = s.updated_at
        WHEN NOT MATCHED THEN INSERT *
        """
    )
