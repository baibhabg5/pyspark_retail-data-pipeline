from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("Verify Silver")
    .getOrCreate()
)

df = spark.read.parquet(
    "/opt/spark-data/silver/retail_sales_clean.parquet"
)

print("Count:", df.count())

df.groupBy("product_category").count().show()

df.groupBy("payment_type").count().show()

df.groupBy("gender").count().show()