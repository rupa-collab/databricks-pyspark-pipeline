# Databricks notebook source
# COMMAND ----------
# POC 2: Incremental Loads + CDC Merge + SCD2
# Goal: simulate daily drops, upsert to silver, maintain SCD2 dimension

# COMMAND ----------
# Run 00_setup_pocs first or paste its cells here.

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# COMMAND ----------
# Banking schema (PaySim-structured, Cifer Fraud Detection dataset)

paysim_schema = StructType([
    StructField("step", IntegerType(), True),
    StructField("type", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("nameOrig", StringType(), True),
    StructField("oldbalanceOrg", DoubleType(), True),
    StructField("newbalanceOrig", DoubleType(), True),
    StructField("nameDest", StringType(), True),
    StructField("oldbalanceDest", DoubleType(), True),
    StructField("newbalanceDest", DoubleType(), True),
    StructField("isFraud", IntegerType(), True),
    StructField("isFlaggedFraud", IntegerType(), True),
])

# COMMAND ----------
# Read raw PaySim data

raw_df = (spark.read.format("csv")
    .option("header", True)
    .schema(paysim_schema)
    .load(RAW_CUSTOMERS_PATH)
)

base_ts = F.unix_timestamp(F.lit(BANKING_START_DATE))

bank_df = (raw_df
    .withColumn("event_ts", F.from_unixtime(base_ts + F.col("step") * F.lit(3600)).cast("timestamp"))
    .withColumn("ingest_date", F.to_date("event_ts"))
    .withColumn("txn_id", F.sha2(F.concat_ws("||",
        F.col("step"), F.col("type"), F.col("amount"), F.col("nameOrig"), F.col("nameDest")
    ), 256))
)

# COMMAND ----------
# Simulate an incremental batch

BATCH_DATE = "2017-01-10"  # change per run

batch_df = bank_df.filter(F.col("ingest_date") == F.lit(BATCH_DATE).cast("date"))

# COMMAND ----------
# Upsert into silver (CDC merge)

silver_path = table_path("silver_bank_txn")

if not DeltaTable.isDeltaTable(spark, silver_path):
    batch_df.write.format("delta").mode("overwrite").save(silver_path)
else:
    silver = DeltaTable.forPath(spark, silver_path)
    (silver.alias("t")
        .merge(batch_df.alias("s"), "t.txn_id = s.txn_id")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

write_job_metric("poc2_silver_upsert_rows", float(batch_df.count()), {"batch_date": BATCH_DATE})

# COMMAND ----------
# SCD Type 2 for account balances (nameOrig)

scd2_path = table_path("gold_account_dim")

updates = (batch_df
    .select(
        F.col("nameOrig").alias("account_id"),
        F.col("newbalanceOrig").alias("balance"),
        F.col("type").alias("last_txn_type"),
        F.col("event_ts").alias("updated_at"),
    )
)

window = Window.partitionBy("account_id").orderBy(F.col("updated_at").desc())
latest_updates = updates.withColumn("rn", F.row_number().over(window)).filter(F.col("rn") == 1).drop("rn")

if not DeltaTable.isDeltaTable(spark, scd2_path):
    scd2_df = (latest_updates
        .withColumn("effective_start", F.col("updated_at"))
        .withColumn("effective_end", F.lit(None).cast("timestamp"))
        .withColumn("is_current", F.lit(True))
    )
    scd2_df.write.format("delta").mode("overwrite").save(scd2_path)
else:
    dim = DeltaTable.forPath(spark, scd2_path)

    (dim.alias("t")
        .merge(latest_updates.alias("s"), "t.account_id = s.account_id AND t.is_current = true")
        .whenMatchedUpdate(set={
            "effective_end": "s.updated_at",
            "is_current": "false",
        })
        .whenNotMatchedInsert(values={
            "account_id": "s.account_id",
            "balance": "s.balance",
            "last_txn_type": "s.last_txn_type",
            "updated_at": "s.updated_at",
            "effective_start": "s.updated_at",
            "effective_end": "NULL",
            "is_current": "true",
        })
        .execute()
    )

write_job_metric("poc2_scd2_rows", float(latest_updates.count()), {"batch_date": BATCH_DATE})

# COMMAND ----------
# Display key outputs (Serverless-friendly)

display(batch_df.limit(5))
display(latest_updates.limit(5))
