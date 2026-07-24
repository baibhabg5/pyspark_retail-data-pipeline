from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("Validate Bronze")
    .getOrCreate()
)

BRONZE_PATH = "/opt/spark-data/bronze/retail_sales_bronze.parquet"

print("VALIDATING BRONZE LAYER")

# ------------------------------------
# Read Bronze Layer
# ------------------------------------
bronze_df = spark.read.parquet(BRONZE_PATH)

# ------------------------------------
# Basic Statistics
# ------------------------------------
print(f"\nTotal Rows: {bronze_df.count()}")
print(f"Total Columns: {len(bronze_df.columns)}")

# ------------------------------------
# Schema Validation
# ------------------------------------
print("\nSchema:")
bronze_df.printSchema()

# ------------------------------------
# Partition Validation
# ------------------------------------
print("\nAvailable ingestion_date partitions:")

bronze_df.select("ingestion_date") \
         .distinct() \
         .orderBy("ingestion_date") \
         .show(truncate=False)

# ------------------------------------
# Null Counts
# ------------------------------------
print("\nNull Counts:")

for column in bronze_df.columns:
    null_count = bronze_df.filter(
        bronze_df[column].isNull()
    ).count()

    print(f"{column}: {null_count}")

# ------------------------------------
# Sample Data
# ------------------------------------
print("\nSample Records:")

bronze_df.show(10, truncate=False)

# ------------------------------------
# Record Count by Status
# ------------------------------------
print("\nOrder Status Distribution:")

bronze_df.groupBy("order_status") \
         .count() \
         .show()

# ------------------------------------
# Record Count by Product Category
# ------------------------------------
print("\nProduct Category Distribution:")

bronze_df.groupBy("product_category") \
         .count() \
         .show()

print("BRONZE LAYER VALIDATION COMPLETED")

spark.stop()