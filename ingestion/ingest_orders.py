from common.logger import get_logger

logger = get_logger("BronzeIngestion")

def ingest_orders(spark, raw_path, bronze_path):
    logger.info("Reading raw orders")

    df = spark.read.format("csv")         .option("header", True)         .option("inferSchema", True)         .load(raw_path)

    logger.info("Writing to bronze delta table")

    df.write.format("delta")         .mode("append")         .save(bronze_path)

    logger.info("Bronze ingestion completed")
