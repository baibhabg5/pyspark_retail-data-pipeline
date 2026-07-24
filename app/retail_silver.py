from pyspark.sql import SparkSession


from pyspark.sql.functions import (
    col, when, upper, trim
)
from pyspark.sql.functions import current_timestamp
spark = (
    SparkSession.builder
    .appName("Retail Silver Layer")
    .getOrCreate()
)
# ------------------------------------
# Read Bronze Layer
# ------------------------------------
bronze_df = spark.read.parquet(
    "/opt/spark-data/bronze/retail_sales_bronze.parquet"
)

print("Bronze count:", bronze_df.count())

bronze_df.printSchema()

# ------------------------------------
# Deduplicate the transaction_id
# ------------------------------------
bronze_df.groupBy("transaction_id").count().filter(col("count") > 1).show(5, truncate=False) # Show some duplicate transaction IDs to understand the extent of duplication

silver_df = bronze_df.dropDuplicates(["transaction_id"])
silver_df = silver_df.filter(
    col("transaction_id").isNotNull()
)# Drop records with null transaction_id as they cannot be uniquely identified and may cause issues in downstream[later stages/layers] processing.
print("Silver countafter deduplication:", silver_df.count())
#-------------------------------------
#Drop the null customer_ids
#------------------------------------
silver_df = silver_df.filter(
    col("customer_id").isNotNull()
)# Drop records with null customer_id.

#------------------------------------
# Validate Product Categories [important data validation for business use cases]
#------------------------------------
allowed_categories = [
    "Electronics",
    "Fashion",
    "Grocery",
    "Sports"
]
before=silver_df.count()
# Furniture category was discontinued by the business due to
# high logistics and warehousing costs.
# Only active product categories are retained in Silver.

silver_df = silver_df.filter(
    col("product_category").isin(allowed_categories)
)
after=silver_df.count()
print(f"Records removed due to discontinued category: {before - after}")

# ------------------------------------
# Date Corrections
# ------------------------------------
silver_df.filter(col("ship_date") < col("order_date")).show(5)#Ship date before order date is a data quality issue.

silver_df = silver_df.withColumn(
    "ship_date",
    when(col("ship_date") < col("order_date"), None)
    .otherwise(col("ship_date"))
)
silver_df = silver_df.withColumn(
    "order_status",
    when(
        col("order_status").isin(
            "Delivered",
            "Cancelled",
            "Returned"
        ),
        col("order_status")
    ).otherwise(None)
)#Only 3 order statuses are valid Delivered,Cancelled,Returned

# ------------------------------------
# Quantity & Price Cleaning
# ------------------------------------
silver_df.filter(col("quantity") <= 0).show(5)# Quantity less than or equal to 0 is not valid.

silver_df = silver_df.filter(col("quantity") > 0)

silver_df.filter(col("unit_price") <= 0).show(5)# Unit price less than or equal to 0 is invalid.

silver_df = silver_df.withColumn(
    "unit_price",
    when(col("unit_price") <= 0, None)
    .otherwise(col("unit_price"))
)

# ------------------------------------
# Discount Cleaning
# ------------------------------------
silver_df.filter(
    (col("discount_pct") < 0) | (col("discount_pct") > 100)
).show(5)  # Discount percentage less than 0% or greater than 100% is not realistic in most retail scenarios.

silver_df = silver_df.withColumn(
    "discount_pct",
    when((col("discount_pct") < 0) | (col("discount_pct") > 100), None)
    .otherwise(col("discount_pct"))
)

# ------------------------------------
# Customer Age Cleaning
# ------------------------------------
silver_df.filter(
    (col("customer_age") < 15) | (col("customer_age") > 100)
).show(5)  # Customer age less than 15 or greater than 100 is not allowed.

silver_df = silver_df.withColumn(
    "customer_age",
    when((col("customer_age") < 15) | (col("customer_age") > 100), None)
    .otherwise(col("customer_age"))
)

# ------------------------------------
# Standardize Gender
# ------------------------------------
silver_df.groupBy("gender").count().show()# Show the distinct gender values to understand the variations and decide on how to standardize them.
silver_df = silver_df.withColumn(
    "gender",
    upper(trim(col("gender")))
)#m,M,male,MALE should all be standardized to M and similarly for F.
silver_df = silver_df.withColumn(
    "gender",
    when(col("gender") == "MALE", "M")
    .when(col("gender") == "FEMALE", "F")
    .when(col("gender").isin("M", "F"), col("gender"))
    .otherwise(None)
)#only M and F are valid else None

# ------------------------------------
# Standardize Payment Type
# ------------------------------------
silver_df.filter(
    ~col("payment_type").isin("CARD", "UPI", "COD")
).show(5)  # Show payment types that are not in the allowed list[except CARD,UPI,COD]
silver_df = silver_df.withColumn(
    "payment_type",
    upper(trim(col("payment_type")))
)#card,CARD,upi,UPI, cod ,COD should all be standardized to Card,UPI,COD respectively
silver_df = silver_df.withColumn(
    "payment_type",
    when(col("payment_type").isin("CARD", "UPI", "COD"), col("payment_type"))
    .otherwise(None)
)
#-------------------------------------
#Auditing Columns
#--------------------------------

silver_df = silver_df.withColumn(
    "silver_processed_at",
    current_timestamp()
)
#------------------------------------
#Data Quality Metrics
#------------------------------------
print("Null payment types:",
      silver_df.filter(
          col("payment_type").isNull()
      ).count())

print("Null gender:",
      silver_df.filter(
          col("gender").isNull()
      ).count())

print("Null unit_price:",
      silver_df.filter(
          col("unit_price").isNull()
      ).count())

print("Silver count:", silver_df.count())

# ------------------------------------
# Write Silver Layer
# ------------------------------------
(
    silver_df
    .repartition("order_status")
    .write
    .mode("overwrite")
    .partitionBy("order_status")
    .parquet("/opt/spark-data/silver/retail_sales_clean.parquet")
)#difference between partitionBy(while saving) and repartition(Before complex operations (like joins)) is that partitionBy is used to physically partition the data on disk based on the specified column(s) when writing,
#while repartition is used to change the number of partitions in the DataFrame in memory for processing.
# In this case, we are repartitioning the DataFrame by "order_status" before writing it to disk, which can help optimize the write operation and subsequent reads.

print("Silver layer created successfully")
spark.stop()
