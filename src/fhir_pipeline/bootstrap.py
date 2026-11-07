"""One-time setup: schemas, raw volume and metadata tables."""

from __future__ import annotations

from typing import Any, Dict

from pyspark.sql import SparkSession

from .audit import ensure_meta_tables
from .config import raw_root


def ensure_storage(spark: SparkSession, cfg: Dict[str, Any]) -> Dict[str, str]:
    storage = cfg["storage"]
    catalog = storage["catalog"]

    for layer, schema in storage["schemas"].items():
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

    if not storage.get("raw_root_override"):
        spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{storage['schemas']['raw']}.{storage['raw_volume']}")

    ensure_meta_tables(spark, cfg)
    return {"raw_root": raw_root(cfg), **{k: f"{catalog}.{v}" for k, v in storage["schemas"].items()}}
