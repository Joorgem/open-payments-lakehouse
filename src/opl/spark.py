# src/opl/spark.py
"""Local Delta-enabled SparkSession factory. Mirrors the config Databricks
applies automatically, so code tested here behaves the same on DBR 16.4.

This is the ONE place that describes the session this project runs against
locally, which is why the resource declarations below live here rather than in
the pytest fixture that wraps it: a fixture configuring the session differently
would make this module's claim false for every test that reads it.
"""
import os
import sys

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession


def _pin_worker_interpreter() -> None:
    """Make Spark's Python workers run the interpreter the driver runs.

    Measured on this machine, not theoretical: with PYSPARK_PYTHON unset, Spark
    launches its workers from the first `python` on PATH -- 3.14 -- against a
    3.12 driver, and every Spark test dies inside py4j with
    ``AssertionError: SRE module mismatch``, a message naming neither Python nor
    PATH and pointing at neither.

    ``sys.executable`` rather than a hardcoded path, so it is right in the venv,
    in CI, and in whatever environment comes next. Set here rather than in the
    test fixture because it has to be set BEFORE a SparkContext exists -- that is
    when the worker command is captured -- and this function is what creates it.
    """
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)


def local_session(app_name: str = "opl") -> SparkSession:
    _pin_worker_interpreter()
    builder = (
        SparkSession.builder.appName(app_name)
        # local[2] and an explicit driver memory DECLARE what this session needs
        # instead of leaving it implicit. The failure they make legible: at
        # ~1.8 GB free, 14 tests fail with a Delta BlockManager error that never
        # mentions memory, while the same commit is green in CI. local[*] gives
        # one executor slot per core, which multiplies the memory each shuffle
        # wants on a laptop that is also running an IDE -- and nothing here is
        # throughput-bound, so the slots buy nothing back.
        .master("local[2]")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.warehouse.dir", "./spark-warehouse")
        .config("spark.ui.enabled", "false")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()
