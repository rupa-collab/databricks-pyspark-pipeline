# Databricks notebook source
# COMMAND ----------
# Shared setup for PySpark POCs (Databricks)

DB_NAME = "pyspark_pocs"
BASE_PATH = "dbfs:/tmp/pyspark_pocs"

# Retail dataset (UCI Online Retail). CSV export staged in UC Volumes.
RAW_ORDERS_PATH = "dbfs:/Volumes/workspace/default/datasets/online_retail/online_retail.csv"

# Banking dataset (Cifer Fraud Detection, PaySim-structured). CSV staged in UC Volumes.
RAW_CUSTOMERS_PATH = "dbfs:/Volumes/workspace/default/datasets/cifer_fraud/Cifer-Fraud-Detection-Dataset-AF-part-1-14.csv"

# Optional products dataset. Leave as default to derive from retail data.
RAW_PRODUCTS_PATH = ""

# Base date for step -> timestamp conversion (1 step = 1 hour).
BANKING_START_DATE = "2017-01-01"

# COMMAND ----------
# Database and paths

spark.sql(f"CREATE DATABASE IF NOT EXISTS {DB_NAME}")
spark.sql(f"USE {DB_NAME}")


def table_path(name: str) -> str:
    return f"{BASE_PATH}/{name}"


METRICS_TABLE = f"{DB_NAME}.metrics_job"
QUALITY_TABLE = f"{DB_NAME}.metrics_quality"

# COMMAND ----------
# Metrics helpers

from pyspark.sql import functions as F


def write_job_metric(metric_name: str, metric_value: float, context: dict | None = None):
    context = context or {}
    rows = [(metric_name, float(metric_value), str(context))]
    df = spark.createDataFrame(rows, ["metric_name", "metric_value", "context_json"])
    df = df.withColumn("recorded_at", F.current_timestamp())
    df.write.format("delta").mode("append").saveAsTable(METRICS_TABLE)


def write_quality_metrics(quality_rows):
    df = spark.createDataFrame(quality_rows, ["metric_name", "metric_value", "context_json"])
    df = df.withColumn("recorded_at", F.current_timestamp())
    df.write.format("delta").mode("append").saveAsTable(QUALITY_TABLE)

# COMMAND ----------
# Quality helpers

from pyspark.sql import DataFrame


def quarantine_invalid(df: DataFrame, required_cols: list[str], key_cols: list[str]):
    required_expr = None
    for col_name in required_cols:
        expr = F.col(col_name).isNull()
        required_expr = expr if required_expr is None else (required_expr | expr)

    dup_df = df.groupBy([F.col(c) for c in key_cols]).count().filter(F.col("count") > 1)
    dup_keys = dup_df.select(*key_cols).withColumn("is_duplicate", F.lit(True))

    invalid_df = df.filter(required_expr) if required_expr is not None else df.limit(0)
    invalid_df = invalid_df.withColumn("is_missing_required", F.lit(True))

    deduped_df = df.dropDuplicates(key_cols)
    return deduped_df, invalid_df, dup_keys
