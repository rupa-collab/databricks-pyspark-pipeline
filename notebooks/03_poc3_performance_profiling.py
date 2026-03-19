# Databricks notebook source
# COMMAND ----------
# POC 3: Performance + Cost Profiling
# Goal: demonstrate measurable tuning for joins and aggregations

# COMMAND ----------
# Run 00_setup_pocs first or paste its cells here.

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
import time

# COMMAND ----------
# Load or create fact/dim datasets

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

silver_orders_path = table_path("silver_orders")

try:
    orders_df = spark.read.format("delta").load(silver_orders_path)
except Exception:
    raw_df = (spark.read.format("csv")
        .option("header", True)
        .schema(retail_schema)
        .load(RAW_ORDERS_PATH)
    )
    raw_df = raw_df.withColumn("InvoiceDate", F.to_timestamp("InvoiceDate", "M/d/yyyy H:mm"))
    orders_df = (raw_df
        .withColumn("order_id", F.col("InvoiceNo"))
        .withColumn("customer_id", F.col("CustomerID"))
        .withColumn("product_id", F.col("StockCode"))
        .withColumn("order_ts", F.col("InvoiceDate"))
        .withColumn("qty", F.col("Quantity"))
        .withColumn("amount", F.col("Quantity") * F.col("UnitPrice"))
    )

# Small dim table derived from retail data
if RAW_PRODUCTS_PATH:
    dim_products = (spark.read.format("csv")
        .option("header", True)
        .load(RAW_PRODUCTS_PATH)
        .select("product_id", "category")
        .dropDuplicates(["product_id"])
    )
else:
    dim_products = (orders_df
        .select("product_id")
        .distinct()
        .withColumn("category", F.substring(F.col("product_id"), 1, 1))
    )

# COMMAND ----------
# Baseline join (no broadcast)

try:
    spark.conf.set("spark.sql.adaptive.enabled", "true")
except Exception as exc:
    display(f"Adaptive execution config not available: {exc}")
start = time.time()
join_df = orders_df.join(dim_products, on="product_id", how="left")
join_df.groupBy("category").agg(F.sum("amount").alias("total_sales")).count()

write_job_metric("poc3_join_baseline_sec", time.time() - start, {"mode": "no_broadcast"})

# COMMAND ----------
# Broadcast join

start = time.time()
join_df = orders_df.join(F.broadcast(dim_products), on="product_id", how="left")
join_df.groupBy("category").agg(F.sum("amount").alias("total_sales")).count()

write_job_metric("poc3_join_broadcast_sec", time.time() - start, {"mode": "broadcast"})

# COMMAND ----------
# Partition tuning

spark.conf.set("spark.sql.shuffle.partitions", "200")
start = time.time()
orders_df.repartition(200, "product_id") \
    .groupBy("product_id").agg(F.sum("amount").alias("total_sales")).count()
write_job_metric("poc3_partition_200_sec", time.time() - start, {"partitions": 200})

spark.conf.set("spark.sql.shuffle.partitions", "50")
start = time.time()
orders_df.repartition(50, "product_id") \
    .groupBy("product_id").agg(F.sum("amount").alias("total_sales")).count()
write_job_metric("poc3_partition_50_sec", time.time() - start, {"partitions": 50})

# COMMAND ----------
# Caching (Serverless may not support persist/cache)

try:
    orders_cached = orders_df.cache()
    orders_cached.count()
    cached_flag = True
except Exception as exc:
    display(f"Caching not supported on this compute: {exc}")
    orders_cached = orders_df
    cached_flag = False

start = time.time()
orders_cached.groupBy("product_id").agg(F.sum("amount").alias("total_sales")).count()
write_job_metric("poc3_cached_agg_sec", time.time() - start, {"cached": cached_flag})

# COMMAND ----------
# Display key outputs (Serverless-friendly)

display(dim_products.limit(5))
