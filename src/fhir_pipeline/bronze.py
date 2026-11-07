"""Bronze layer: raw API pages -> one row per FHIR resource version, stored as Delta.

Each row keeps the resource exactly as received (resource_payload) plus a JSON
copy (resource_json). For JSON loads both are the same string. For XML loads the
JSON copy is produced with the field spec so silver can use one parsing path.
"""

from __future__ import annotations

import json
import xml.etree.ElementTree as ET
from typing import Any, Dict, List

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from .audit import successful_files
from .config import table_name
from .fhir_schema import xml_resource_to_dict

BUNDLE_DDL = "entry array<struct<fullUrl:string,resource:string>>"

BRONZE_KEYS = ["run_id", "resource_id", "version_id"]

_XML_ROW_SCHEMA = StructType(
    [
        StructField("source_file", StringType()),
        StructField("full_url", StringType()),
        StructField("resource_payload", StringType()),
        StructField("resource_json", StringType()),
    ]
)


def _norm_path(col: Any) -> Any:
    """dbfs:/Volumes/x and /Volumes/x should compare equal."""
    return F.regexp_replace(col, r"^(dbfs:|file:)", "")


def _json_entries(spark: SparkSession, paths: List[str]) -> DataFrame:
    raw = (
        spark.read.format("text")
        .option("wholetext", "true")
        .load(paths)
        .select(F.col("value").alias("body"), _norm_path(F.col("_metadata.file_path")).alias("source_file"))
    )
    bundle = raw.withColumn("b", F.from_json("body", BUNDLE_DDL))
    return (
        bundle.select("source_file", F.explode("b.entry").alias("e"))
        .select(
            "source_file",
            F.col("e.fullUrl").alias("full_url"),
            F.col("e.resource").alias("resource_payload"),
        )
        .withColumn("resource_json", F.col("resource_payload"))
    )


def _xml_entries(spark: SparkSession, paths: List[str]) -> DataFrame:
    """XML pages are split on the driver, volumes here are small (a few thousand resources)."""
    rows = []
    for path in paths:
        with open(path, "r", encoding="utf-8") as fh:
            root = ET.fromstring(fh.read())
        for entry in root:
            if entry.tag.split("}")[-1] != "entry":
                continue
            full_url = None
            resource_elem = None
            for child in entry:
                name = child.tag.split("}")[-1]
                if name == "fullUrl":
                    full_url = child.get("value")
                if name == "resource" and len(child):
                    resource_elem = child[0]
            if resource_elem is None:
                continue
            as_dict = xml_resource_to_dict(resource_elem)
            rows.append(
                (
                    path,
                    full_url,
                    ET.tostring(resource_elem, encoding="unicode"),
                    json.dumps(as_dict, ensure_ascii=False),
                )
            )
    return spark.createDataFrame(rows, schema=_XML_ROW_SCHEMA)


def build_bronze_df(spark: SparkSession, cfg: Dict[str, Any], run_id: str, resource: str, payload_format: str) -> DataFrame:
    calls = successful_files(spark, cfg, run_id, resource)
    call_rows = calls.select(
        _norm_path(F.col("file_path")).alias("source_file"),
        F.col("request_url").alias("api_url_or_params"),
        F.col("called_at").alias("extraction_timestamp"),
        F.col("saved_at"),
        F.col("page_number"),
        F.col("window_start"),
    ).cache()

    paths = [r["source_file"] for r in call_rows.select("source_file").distinct().collect()]
    if not paths:
        return None

    entries = _json_entries(spark, paths) if payload_format == "json" else _xml_entries(spark, paths)
    entries = entries.withColumn("source_file", _norm_path(F.col("source_file")))

    df = (
        entries.join(call_rows, "source_file", "left")
        .withColumn("resource_type", F.get_json_object("resource_json", "$.resourceType"))
        .where(F.col("resource_type") == resource)
        .withColumn("resource_id", F.get_json_object("resource_json", "$.id"))
        .withColumn("version_id", F.coalesce(F.get_json_object("resource_json", "$.meta.versionId"), F.lit("0")))
        .withColumn("last_updated", F.to_timestamp(F.get_json_object("resource_json", "$.meta.lastUpdated")))
        .withColumn("row_hash", F.sha2(F.col("resource_json"), 256))
        .withColumn("payload_format", F.lit(payload_format))
        .withColumn("run_id", F.lit(run_id))
        .withColumn("ingest_date", F.to_date("window_start"))
        .withColumn("bronze_loaded_at", F.current_timestamp())
        .select(
            "resource_type",
            "resource_id",
            "version_id",
            "last_updated",
            "full_url",
            "payload_format",
            "resource_payload",
            "resource_json",
            "row_hash",
            "api_url_or_params",
            "extraction_timestamp",
            "saved_at",
            "page_number",
            "source_file",
            "run_id",
            "ingest_date",
            "bronze_loaded_at",
        )
    )
    return df


def load_bronze(spark: SparkSession, cfg: Dict[str, Any], run_id: str, resource: str, payload_format: str) -> Dict[str, int]:
    """Insert-only merge, so re-running the same run_id never duplicates rows."""
    df = build_bronze_df(spark, cfg, run_id, resource, payload_format)
    if df is None:
        return {"rows_read": 0, "rows_written": 0}

    target = table_name(cfg, "bronze", resource)
    df = df.cache()
    rows_read = df.count()

    if not spark.catalog.tableExists(target):
        df.write.format("delta").saveAsTable(target)
        return {"rows_read": rows_read, "rows_written": rows_read}

    from delta.tables import DeltaTable

    cond = " AND ".join(f"t.{k} = s.{k}" for k in BRONZE_KEYS)
    (
        DeltaTable.forName(spark, target)
        .alias("t")
        .merge(df.alias("s"), cond)
        .whenNotMatchedInsertAll()
        .execute()
    )
    return {"rows_read": rows_read, "rows_written": rows_read}
