"""Silver layer: parse, flatten, clean, de-duplicate and keep history with SCD2."""

from __future__ import annotations

from typing import Any, Dict, Optional

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

from .config import table_name
from .scd2 import add_row_hash, apply_scd2
from .silver_models import model_for


def flatten_bronze(bronze: DataFrame, resource: str) -> DataFrame:
    """Parse resource_json and map it to the flat silver columns, trimmed and de-duplicated per key."""
    model = model_for(resource)
    key = model["key"]

    parsed = bronze.withColumn("r", F.from_json("resource_json", model["ddl"]))
    flat = parsed.select(
        *[F.expr(expr).alias(name) for name, expr in model["columns"]],
        F.col("version_id").alias("source_version_id"),
        F.col("last_updated").alias("source_last_updated"),
        F.col("extraction_timestamp"),
        F.col("run_id").alias("source_run_id"),
    )

    # basic cleaning: trim text, empty string -> NULL
    for field in flat.schema.fields:
        if isinstance(field.dataType, StringType) and field.name not in ("source_run_id", "source_version_id"):
            flat = flat.withColumn(field.name, F.nullif(F.trim(F.col(field.name)), F.lit("")))

    # one row per key: latest version wins when the same resource appears more than once in a load
    latest = Window.partitionBy(key).orderBy(
        F.col("source_last_updated").desc_nulls_last(),
        F.col("source_version_id").cast("int").desc_nulls_last(),
        F.col("extraction_timestamp").desc_nulls_last(),
    )
    dedup = flat.withColumn("_rn", F.row_number().over(latest)).where("_rn = 1").drop("_rn")

    staged = dedup.withColumn("effective_from", F.coalesce("source_last_updated", "extraction_timestamp"))
    return add_row_hash(staged, model["business_columns"])


def build_staged(spark: SparkSession, cfg: Dict[str, Any], resource: str, run_id: str) -> DataFrame:
    bronze = spark.table(table_name(cfg, "bronze", resource)).where(
        (F.col("run_id") == run_id) & F.col("resource_id").isNotNull()
    )
    return flatten_bronze(bronze, resource)


def load_silver(spark: SparkSession, cfg: Dict[str, Any], resource: str, run_id: str) -> Dict[str, Any]:
    model = model_for(resource)
    staged = build_staged(spark, cfg, resource, run_id)
    if staged.limit(1).count() == 0:
        return {"rows_read": 0, "new_keys": 0, "new_versions": 0, "unchanged": 0, "inserted_rows": 0}

    rows_read = staged.count()
    stats = apply_scd2(spark, staged, table_name(cfg, "silver", resource), model["key"])
    stats["rows_read"] = rows_read
    return stats
