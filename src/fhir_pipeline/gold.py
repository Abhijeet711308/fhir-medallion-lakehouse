"""Gold layer: reporting views (from sql/gold_views.sql) and a date dimension table."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from .config import table_name

_SQL_FILE = Path(__file__).resolve().parents[2] / "sql" / "gold_views.sql"


def render_statements(cfg: Dict[str, Any], sql_path: Path = _SQL_FILE) -> List[str]:
    storage = cfg["storage"]
    values = {
        "catalog": storage["catalog"],
        "silver": storage["schemas"]["silver"],
        "gold": storage["schemas"]["gold"],
        "meta": storage["schemas"]["meta"],
        "run_log": cfg["tables"]["run_log"],
        "api_call_log": cfg["tables"]["api_call_log"],
    }
    text = sql_path.read_text(encoding="utf-8")
    lines = [ln for ln in text.splitlines() if not ln.strip().startswith("--")]
    statements = [s.strip() for s in "\n".join(lines).split(";") if s.strip()]
    return [s.format(**values) for s in statements]


def build_dim_date(spark: SparkSession, cfg: Dict[str, Any]) -> int:
    """Calendar table from the earliest fact date to today (plus a small margin).

The public test server holds records dated far in the future (e.g. year 2137); those
are flagged in the facts (is_future_dated) and kept out of the calendar so it stays small.
"""
    gold = cfg["storage"]["schemas"]["gold"]
    catalog = cfg["storage"]["catalog"]
    bounds = spark.sql(
        f"""
        SELECT MIN(d) AS lo, LEAST(MAX(d), current_date()) AS hi FROM (
            SELECT encounter_date AS d FROM {catalog}.{gold}.fact_encounter
            UNION ALL SELECT effective_date FROM {catalog}.{gold}.fact_observation
            UNION ALL SELECT onset_date FROM {catalog}.{gold}.fact_condition
        ) WHERE d IS NOT NULL
        """
    ).collect()[0]
    if bounds["lo"] is None:
        return 0

    df = (
        spark.sql(
            f"SELECT explode(sequence(date_sub(DATE'{bounds['lo']}', 7), date_add(DATE'{bounds['hi']}', 7), interval 1 day)) AS date_key"
        )
        .withColumn("year", F.year("date_key"))
        .withColumn("quarter", F.quarter("date_key"))
        .withColumn("month", F.month("date_key"))
        .withColumn("month_name", F.date_format("date_key", "MMMM"))
        .withColumn("day", F.dayofmonth("date_key"))
        .withColumn("day_of_week", F.dayofweek("date_key"))
        .withColumn("day_name", F.date_format("date_key", "EEEE"))
        .withColumn("is_weekend", F.dayofweek("date_key").isin(1, 7))
    )
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
        table_name(cfg, "gold", "dim_date")
    )
    return df.count()


def build_gold(spark: SparkSession, cfg: Dict[str, Any]) -> Dict[str, int]:
    statements = render_statements(cfg)
    for stmt in statements:
        spark.sql(stmt)
    dates = build_dim_date(spark, cfg)
    return {"views_created": len(statements), "dim_date_rows": dates}
