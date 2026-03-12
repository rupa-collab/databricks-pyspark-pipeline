from common.utils import remove_negative_orders, drop_duplicate_orders
from common.logger import get_logger

logger = get_logger("SilverTransformation")

def clean_orders(spark, bronze_path, silver_path):

    logger.info("Reading bronze orders")

    df = spark.read.format("delta").load(bronze_path)

    logger.info("Removing negative values")
    df = remove_negative_orders(df)

    logger.info("Removing duplicates")
    df = drop_duplicate_orders(df)

    logger.info("Writing clean data to silver layer")

    df.write.format("delta")         .mode("overwrite")         .save(silver_path)

    logger.info("Silver transformation completed")
