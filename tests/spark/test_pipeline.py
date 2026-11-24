"""Ingest generated files end to end with Spark file streaming (the CI stand-in for Auto Loader)."""

import os
from functools import reduce
from pathlib import Path

import pytest

pytest.importorskip("pyspark")
pytest.importorskip("delta")

from alpatterns.config import load_config  # noqa: E402
from alpatterns.generate import generate  # noqa: E402
from alpatterns.ingest import ingest  # noqa: E402
from alpatterns.standardize import split_quarantine, standardize  # noqa: E402

REPO = Path(__file__).resolve().parents[2]


@pytest.fixture(scope="module")
def env(tmp_path_factory):
    try:
        from delta import configure_spark_with_delta_pip
        from pyspark.sql import SparkSession
        root = tmp_path_factory.mktemp("al")
        b = (SparkSession.builder.master("local[2]").appName("autoloader-patterns")
             .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
             .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
             .config("spark.sql.warehouse.dir", str(root / "wh"))
             .config("spark.sql.shuffle.partitions", "2").config("spark.ui.enabled", "false"))
        spark = configure_spark_with_delta_pip(b).getOrCreate()
    except Exception as exc:  # noqa: BLE001
        if os.environ.get("CI"):
            raise                                  # in CI a missing Spark is a failure, not a skip
        pytest.skip(f"Spark not available: {exc}")
    spark.sql("CREATE SCHEMA IF NOT EXISTS bronze")
    landing, checkpoints = root / "landing", root / "chk"
    generate(landing, days=30)
    cfg = load_config(REPO / "config" / "sources.yaml")
    return spark, cfg, str(landing), str(checkpoints)


def run_all(spark, cfg, landing, checkpoints):
    for src in cfg.sources:
        ingest(spark, src, landing, checkpoints, engine="spark")


def test_initial_ingest(env):
    spark, cfg, landing, chk = env
    run_all(spark, cfg, landing, chk)
    assert spark.table("bronze.inventory_northwind").count() == 30 * 40 + 3      # + 3 corrupt lines
    assert spark.table("bronze.inventory_freshmart").count() == 30 * 30
    # merge: the corrected day-14 re-send updated rows instead of adding duplicates
    medline = spark.table("bronze.inventory_medline")
    assert medline.count() == 2 * 30 * 20
    resent = medline.where("manufacturer_id = 'acme_pharma' AND report_date = '2026-03-14'")
    assert resent.where("_source_file LIKE '%_v2.csv'").count() == 20
    assert spark.table("bronze.inventory_medline_json").count() == 30 * 20


def test_corrupt_lines_are_kept_not_dropped(env):
    spark = env[0]
    rescued = spark.table("bronze.inventory_northwind").where("_rescued_data IS NOT NULL")
    assert rescued.count() == 3
    assert rescued.where("_source_file LIKE '%2026032%'").count() == 3          # days 26-28


def test_rerun_processes_nothing_new(env):
    spark, cfg, landing, chk = env
    before = {s.target: spark.table(s.target).count() for s in cfg.sources}
    run_all(spark, cfg, landing, chk)
    assert {s.target: spark.table(s.target).count() for s in cfg.sources} == before


def test_new_files_are_picked_up_incrementally(env):
    spark, cfg, landing, chk = env
    generate(Path(landing), days=1, first_day=31)
    run_all(spark, cfg, landing, chk)
    assert spark.table("bronze.inventory_freshmart").count() == 31 * 30
    assert spark.table("bronze.inventory_northwind").count() == 31 * 40 + 3


def test_standardize_and_quarantine(env):
    spark, cfg = env[0], env[1]
    frames = [standardize(spark.table(s.target), s, cfg) for s in cfg.sources]
    unified = reduce(lambda a, b: a.unionByName(b), frames)
    clean, quarantine = split_quarantine(unified)
    reasons = {r.quarantine_reason: r["count"] for r in quarantine.groupBy("quarantine_reason").count().collect()}
    assert reasons == {"MALFORMED_RECORD": 3, "MISSING_REQUIRED_FIELDS": 11 * 20}   # nested JSON from day 21
    assert clean.count() + quarantine.count() == unified.count()
    assert set(clean.columns) >= set(cfg.unified_columns) | {"source", "_source_file"}
