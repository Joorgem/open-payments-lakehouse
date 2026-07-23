# src/opl/spark.py
"""Local Delta-enabled SparkSession factory. Mirrors the config Databricks
applies automatically, so code tested here behaves the same on DBR 16.4."""
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession


def local_session(app_name: str = "opl") -> SparkSession:
    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.warehouse.dir", "./spark-warehouse")
        .config("spark.ui.enabled", "false")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()
