from pyspark.sql.functions import col

def remove_negative_orders(df):
    return df.filter(col("amount") > 0)

def drop_duplicate_orders(df):
    return df.dropDuplicates(["order_id"])
