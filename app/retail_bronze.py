from pyspark.sql.types import (
    StructType, StructField,
    IntegerType, StringType,
    DoubleType, DateType
)
from pyspark.sql.functions import current_timestamp
from pyspark.sql.functions import input_file_name
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("Retail Bronze")
    .getOrCreate()
)
# ------------------------------------
# Define Explicit Schema
# ------------------------------------
bronze_schema = StructType([
    StructField("transaction_id", IntegerType(), True),
    StructField("order_date", DateType(), True),
    StructField("ship_date", DateType(), True),
    StructField("customer_id", StringType(), True),
    StructField("customer_age", IntegerType(), True),
    StructField("gender", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("product_category", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("unit_price", DoubleType(), True),
    StructField("discount_pct", DoubleType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("payment_type", StringType(), True),
    StructField("order_status", StringType(), True),
    StructField("ingestion_date", DateType(), True)
])

# ------------------------------------
# Read Raw CSV (NO CLEANING)
# ------------------------------------
bronze_df = (
    spark.read
    .option("header", "true")
    .schema(bronze_schema)
    .csv("/opt/spark-data/raw/retail_sales_raw.csv")
)

# ------------------------------------
# Basic Validation
# ------------------------------------
print("Record count:", bronze_df.count())

bronze_df.printSchema()

print("Partitions:", bronze_df.rdd.getNumPartitions())

bronze_df.select("ingestion_date").distinct().show()

# ------------------------------------
# Write to Bronze Layer as Parquet
# ------------------------------------
(
    bronze_df
    .write
    .mode("overwrite")
    .partitionBy("ingestion_date")
    .parquet("/opt/spark-data/bronze/retail_sales_bronze.parquet")
)

print("Bronze layer created successfully")
spark.stop()
'''Interactive shell
pyspark
Spark secretly does:
spark = SparkSession.builder.getOrCreate()
for you.

Production script
spark-submit my_job.py

You must explicitly write:

spark = SparkSession.builder.getOrCreate()

yourself'''