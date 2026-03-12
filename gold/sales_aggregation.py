from pyspark.sql.functions import sum
from common.logger import get_logger

logger = get_logger("GoldAggregation")

def create_sales_aggregation(spark, silver_path, gold_path):

    logger.info("Reading silver data")

    df = spark.read.format("delta").load(silver_path)

    logger.info("Aggregating sales")

    sales_df = df.groupBy("product_id")         .agg(sum("amount").alias("total_sales"))

    logger.info("Writing gold analytics table")

    sales_df.write.format("delta")         .mode("overwrite")         .save(gold_path)

    logger.info("Gold layer created successfully")
