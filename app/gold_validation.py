from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("Validate Gold")
    .getOrCreate()
)
GOLD_PATH = "/opt/spark-data/gold"

print("VALIDATING GOLD LAYER")

# --------------------------------------------------
# Daily Sales Metrics
# --------------------------------------------------
print("\n1.DAILY SALES METRICS")

daily_df = spark.read.parquet(
    f"{GOLD_PATH}/daily_sales_metrics.parquet"
)
    
print(f"Rows: {daily_df.count()}")
daily_df.printSchema()
daily_df.show(10, truncate=False)

# --------------------------------------------------
# Product Category Performance
# --------------------------------------------------
print("\n2.PRODUCT CATEGORY PERFORMANCE")

product_df = spark.read.parquet(
    f"{GOLD_PATH}/product_category_performance.parquet"
)

print(f"Rows: {product_df.count()}")
product_df.printSchema()
product_df.show(10, truncate=False)

# --------------------------------------------------
# City Revenue Metrics
# --------------------------------------------------
print("\n3.CITY REVENUE METRICS")

city_df = spark.read.parquet(
    f"{GOLD_PATH}/city_revenue_metrics.parquet"
)

print(f"Rows: {city_df.count()}")
city_df.printSchema()
city_df.show(10, truncate=False)

# --------------------------------------------------
# Monthly Sales Metrics
# --------------------------------------------------
print("\n4.MONTHLY SALES METRICS")

monthly_df = spark.read.parquet(
    f"{GOLD_PATH}/monthly_sales_metrics.parquet"
)

print(f"Rows: {monthly_df.count()}")
monthly_df.printSchema()
monthly_df.show(10, truncate=False)

# --------------------------------------------------
# Top Products
# --------------------------------------------------
print("\n5.TOP PRODUCTS")

top_df = spark.read.parquet(
    f"{GOLD_PATH}/top_products.parquet"
)

print(f"Rows: {top_df.count()}")
top_df.printSchema()
top_df.show(10, truncate=False)

# --------------------------------------------------
# Audit Metadata
# --------------------------------------------------
print("\n6.AUDIT METADATA")

audit_df = spark.read.parquet(
    f"{GOLD_PATH}/audit_metadata.parquet"
)

print(f"Rows: {audit_df.count()}")
audit_df.printSchema()
audit_df.show(truncate=False)


print("GOLD LAYER VALIDATION COMPLETED")


spark.stop()