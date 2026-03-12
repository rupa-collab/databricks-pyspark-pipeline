from pyspark.sql import SparkSession

def test_spark_session():
    spark = SparkSession.builder.master("local").appName("test").getOrCreate()

    data = [(1, 100), (2, 200)]
    df = spark.createDataFrame(data, ["id", "amount"])

    assert df.count() == 2
