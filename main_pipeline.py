import yaml

from common.spark_session import create_spark_session
from ingestion.ingest_orders import ingest_orders
from transformations.clean_orders import clean_orders
from gold.sales_aggregation import create_sales_aggregation

CONFIG_PATH = "configs/config.yaml"

def load_config():
    with open(CONFIG_PATH, 'r') as file:
        return yaml.safe_load(file)

def run_pipeline():
    config = load_config()

    spark = create_spark_session(CONFIG_PATH)

    raw_path = config['paths']['raw_orders']
    bronze_path = config['paths']['bronze_orders']
    silver_path = config['paths']['silver_orders']
    gold_path = config['paths']['gold_sales']

    ingest_orders(spark, raw_path, bronze_path)
    clean_orders(spark, bronze_path, silver_path)
    create_sales_aggregation(spark, silver_path, gold_path)

    spark.stop()

if __name__ == "__main__":
    run_pipeline()
