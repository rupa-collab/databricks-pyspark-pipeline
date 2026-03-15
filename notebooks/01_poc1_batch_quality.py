# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "2"
# ///
# POC 1: Batch Ingestion + Data Quality + Delta Lake
# Goal: raw -> bronze -> silver with quality gates and metrics

# COMMAND ----------

# MAGIC %run ./00_setup_pocs

# COMMAND ----------

# Run 00_setup_pocs first or paste its cells here.

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
import time

# COMMAND ----------

# UCI Online Retail schema (CSV version)

retail_schema = StructType([
    StructField("InvoiceNo", StringType(), True),
    StructField("StockCode", StringType(), True),
    StructField("Description", StringType(), True),
    StructField("Quantity", IntegerType(), True),
    StructField("InvoiceDate", StringType(), True),
    StructField("UnitPrice", DoubleType(), True),
    StructField("CustomerID", StringType(), True),
    StructField("Country", StringType(), True),
])

# COMMAND ----------

# Read raw retail data

start = time.time()
raw_df = (spark.read.format("csv")
    .option("header", True)
    .schema(retail_schema)
    .load(RAW_ORDERS_PATH)
)

raw_df = (raw_df
    .withColumn(
        "InvoiceDate",
        F.coalesce(
            F.expr("try_to_timestamp(InvoiceDate, 'M/d/yyyy H:mm')"),
            F.expr("try_to_timestamp(InvoiceDate, 'd-M-yyyy H:mm')"),
            F.expr("try_to_timestamp(InvoiceDate, 'dd-MM-yyyy HH:mm')")
        )
    )
    .withColumn("InvoiceNo", F.col("InvoiceNo").cast("string"))
    .withColumn("CustomerID", F.col("CustomerID").cast("string"))
    .withColumn("Quantity", F.col("Quantity").cast("int"))
    .withColumn("UnitPrice", F.col("UnitPrice").cast("double"))
)

# Filter out credit notes / cancellations
raw_df = raw_df.filter(~F.col("InvoiceNo").startswith("C"))

orders_df = (raw_df
    .withColumn("order_id", F.col("InvoiceNo"))
    .withColumn("customer_id", F.col("CustomerID"))
    .withColumn("product_id", F.col("StockCode"))
    .withColumn("order_ts", F.col("InvoiceDate"))
    .withColumn("qty", F.col("Quantity"))
    .withColumn("amount", F.col("Quantity") * F.col("UnitPrice"))
    .withColumn("ingest_date", F.current_date())
)

bronze_path = table_path("bronze_orders")
orders_df.write.format("delta").mode("append").save(bronze_path)

write_job_metric("poc1_bronze_write_sec", time.time() - start, {"rows": orders_df.count()})

# COMMAND ----------

# Data quality checks and quarantine

required_cols = ["order_id", "customer_id", "product_id", "order_ts", "amount"]
key_cols = ["order_id", "product_id"]

clean_df, invalid_df, dup_keys = quarantine_invalid(orders_df, required_cols, key_cols)

# Negative amounts check
negative_df = clean_df.filter(F.col("amount") <= 0).withColumn("is_negative_amount", F.lit(True))
clean_df = clean_df.filter(F.col("amount") > 0)

quarantine_df = invalid_df.unionByName(negative_df, allowMissingColumns=True)

quarantine_path = table_path("quarantine_orders")
quarantine_df.write.format("delta").mode("append").save(quarantine_path)

dup_keys.write.format("delta").mode("overwrite").save(table_path("dup_keys_orders"))

quality_rows = [
    ("poc1_total_rows", float(orders_df.count()), "{}"),
    ("poc1_invalid_rows", float(quarantine_df.count()), "{}"),
    ("poc1_duplicate_keys", float(dup_keys.count()), "{}"),
]
write_quality_metrics(quality_rows)

# COMMAND ----------

# Silver write

silver_path = table_path("silver_orders")
clean_df.write.format("delta").mode("overwrite").save(silver_path)

# Delta optimization (Databricks)
try:
    spark.sql(f"OPTIMIZE delta.`{silver_path}` ZORDER BY (product_id)")
except Exception as exc:
    print(f"OPTIMIZE skipped: {exc}")

# COMMAND ----------

# Gold aggregation

gold_path = table_path("gold_sales_by_product")
agg_df = clean_df.groupBy("product_id").agg(F.sum("amount").alias("total_sales"))
agg_df.write.format("delta").mode("overwrite").save(gold_path)

write_job_metric("poc1_gold_rows", float(agg_df.count()), {"table": gold_path})

# COMMAND ----------
# Display key outputs (Serverless-friendly)

display(clean_df.limit(5))
display(quarantine_df.limit(5))
display(dup_keys.limit(5))
display(agg_df.limit(5))
