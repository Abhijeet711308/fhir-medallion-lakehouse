import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))


@pytest.fixture(scope="session")
def spark():
    pyspark = pytest.importorskip("pyspark")
    from pyspark.sql import SparkSession

    s = (
        SparkSession.builder.master("local[2]")
        .appName("fhir-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    yield s
    s.stop()
