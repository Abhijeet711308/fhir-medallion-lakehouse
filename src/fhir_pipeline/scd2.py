"""SCD Type 2 handling for the silver tables.

The change detection is plain DataFrame logic (compute_scd2_changes) so it can be
tested without Delta. apply_scd2 then runs one MERGE that closes the old row and
inserts the new version.

Target columns added on top of the business columns:
    valid_from, valid_to, is_current, version_number, silver_loaded_at
"""

from __future__ import annotations

from typing import Any, Dict, List, Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

SCD_COLUMNS = ["valid_from", "valid_to", "is_current", "version_number", "silver_loaded_at"]


def add_row_hash(df: DataFrame, columns: List[str]) -> DataFrame:
    """Hash over the business columns, NULLs are made visible so NULL != empty string."""
    parts = [F.coalesce(F.col(c).cast("string"), F.lit("<null>")) for c in columns]
    return df.withColumn("row_hash", F.sha2(F.concat_ws("||", *parts), 256))


def compute_scd2_changes(staged: DataFrame, current: DataFrame, key: str) -> Tuple[DataFrame, DataFrame]:
    """
    staged  : latest record per key from this load, with row_hash and effective_from
    current : rows of the target where is_current = true (key, row_hash, valid_from, version_number)

    Returns (new_versions, closers)
      new_versions : rows to insert (brand new keys and changed keys)
      closers      : one row per changed key, used to close the previous version
    """
    cur = current.select(
        F.col(key).alias("_k"),
        F.col("row_hash").alias("_cur_hash"),
        F.col("valid_from").alias("_cur_from"),
        F.col("version_number").alias("_cur_version"),
    )
    joined = staged.join(cur, staged[key] == cur["_k"], "left")

    is_new = F.col("_k").isNull()
    # a changed row must also be newer than what we hold, so a replayed old version is ignored
    is_changed = (
        F.col("_k").isNotNull()
        & (F.col("row_hash") != F.col("_cur_hash"))
        & (F.col("effective_from") > F.col("_cur_from"))
    )
    todo = joined.where(is_new | is_changed)

    new_versions = (
        todo.withColumn("valid_from", F.col("effective_from"))
        .withColumn("valid_to", F.lit(None).cast("timestamp"))
        .withColumn("is_current", F.lit(True))
        .withColumn("version_number", F.coalesce(F.col("_cur_version"), F.lit(0)) + 1)
        .withColumn("silver_loaded_at", F.current_timestamp())
        .drop("_k", "_cur_hash", "_cur_from", "_cur_version")
    )

    closers = todo.where(is_changed).select(
        F.col(key).alias("merge_key"),
        F.col("effective_from").alias("close_at"),
    )
    return new_versions, closers


def apply_scd2(
    spark: SparkSession,
    staged: DataFrame,
    target: str,
    key: str,
) -> Dict[str, Any]:
    """Create the table on first load, otherwise merge new versions in."""
    staged = staged.cache()

    if not spark.catalog.tableExists(target):
        first_load = (
            staged.withColumn("valid_from", F.col("effective_from"))
            .withColumn("valid_to", F.lit(None).cast("timestamp"))
            .withColumn("is_current", F.lit(True))
            .withColumn("version_number", F.lit(1))
            .withColumn("silver_loaded_at", F.current_timestamp())
        )
        n = first_load.count()
        first_load.write.format("delta").saveAsTable(target)
        return {"new_keys": n, "new_versions": 0, "unchanged": 0, "inserted_rows": n}

    from delta.tables import DeltaTable

    tgt = DeltaTable.forName(spark, target)
    current = tgt.toDF().where("is_current")
    new_versions, closers = compute_scd2_changes(staged, current, key)
    new_versions = new_versions.cache()

    target_cols = tgt.toDF().columns
    inserts = new_versions.select(*target_cols).withColumn("merge_key", F.lit(None).cast("string")).withColumn(
        "close_at", F.lit(None).cast("timestamp")
    )
    closes = closers.select("merge_key", "close_at").cache()
    changes = inserts.unionByName(closes, allowMissingColumns=True)

    # counts are taken before the merge, afterwards the "current" snapshot has already moved on
    inserted = new_versions.count()
    changed = closes.count()
    staged_total = staged.count()

    (
        tgt.alias("t")
        .merge(changes.alias("u"), f"t.{key} = u.merge_key AND t.is_current = true")
        .whenMatchedUpdate(set={"is_current": "false", "valid_to": "u.close_at"})
        .whenNotMatchedInsert(values={c: f"u.{c}" for c in target_cols})
        .execute()
    )

    return {
        "new_keys": inserted - changed,
        "new_versions": changed,
        "unchanged": staged_total - inserted,
        "inserted_rows": inserted,
    }
