from pyspark.sql import SparkSession
import yaml

def create_spark_session(config_path):
    with open(config_path, 'r') as file:
        config = yaml.safe_load(file)

    spark = SparkSession.builder         .appName(config['spark']['app_name'])         .config("spark.sql.shuffle.partitions", "200")         .config("spark.databricks.delta.optimizeWrite.enabled", "true")         .config("spark.databricks.delta.autoCompact.enabled", "true")         .getOrCreate()

    return spark
