from pyspark.sql import SparkSession

from pyspark.sql.functions import (
    col, sum, count, avg, round
)
from pyspark.sql.functions import date_format
from pyspark.sql import Row
from datetime import datetime
from pyspark.sql.functions import lower
spark = (
    SparkSession.builder
    .appName("Retail Gold")
    .getOrCreate()
)
# ------------------------------------
# Read Silver Layer
# ------------------------------------
silver_df = spark.read.parquet(
    "/opt/spark-data/silver/retail_sales_clean.parquet"
)

GOLD_PATH = "/opt/spark-data/gold"

silver_df = silver_df.filter(
    lower(col("order_status")) == "Delivered"
)#Only taking delivered orders to avoid inflating revenue with cancelled and returned orders.
# ------------------------------------
# Derived Column: total_amount
# ------------------------------------
silver_df = silver_df.withColumn(
    "total_amount",
    round(col("quantity") * col("unit_price") * (1 - col("discount_pct") / 100), 2)
).cache()
#Calculating the total amount for each transaction after discounnt.
# Cache the transformed Silver dataframe because it is reused
# across multiple aggregations (daily, category, city, monthly, top products).
# Without caching, Spark may recompute the transformation lineage
# for each action.
# #without .cache() lazy evaluation would occur, leading to repeated computations.daily_sales_df = silver_df.groupBy(...),product_perf_df = silver_df.groupBy(...)
# ------------------------------------
#  1.Daily Sales Metrics
# ------------------------------------
daily_sales_df = (
    silver_df
    .groupBy("order_date")
    .agg(
        round(sum("total_amount"), 2).alias("total_revenue"),
        count("transaction_id").alias("total_orders"),
        round(avg("total_amount"), 2).alias("avg_order_value")
    )
)#the daily sales metrics by aggregating the total revenue, total orders, and average order value for each order date.

(
    daily_sales_df
    .coalesce(1)#Coalesce is used to reduce the number of output files to 1 for easier downstream processing and analysis[as it is daily can have many small files].
    .write
    .mode("overwrite")
    .parquet(f"{GOLD_PATH}/daily_sales_metrics.parquet")
)#Writing the daily sales metrics to the Gold layer as a Parquet file.

# ------------------------------------
# 2.2️Product Category Performance
# ------------------------------------
product_perf_df = (
    silver_df
    .groupBy("product_category")
    .agg(
        round(sum("total_amount"), 2).alias("category_revenue"),
        sum("quantity").alias("total_units_sold"),
        count("transaction_id").alias("order_count"),
        round(avg("total_amount"), 2).alias("avg_order_value")#Average Order Value (AOV)What it is: The average amount a customer spends when they buy from this category.
    )
)#Calculating the product category performance for each product category.
#Total Units Sold counts the physical items leaving shelves, which dictates your 
   # inventory stock levels, while Order Count counts checkout receipts, 
   # which dictates your register traffic and staffing needs
(
    product_perf_df
    .write
    .mode("overwrite")
    .parquet(f"{GOLD_PATH}/product_category_performance.parquet")
)#Parquet file creation in Gold layer.

# ------------------------------------
# 3.3️City-Level Revenue Metrics
# ------------------------------------
city_revenue_df = (
    silver_df
    .groupBy("city", "state")
    .agg(
        round(sum("total_amount"), 2).alias("city_revenue"),
        count("transaction_id").alias("order_count"),
        round(avg("total_amount"), 2).alias("avg_order_value")
    )
)#Calculating the city-level revenue metrics for each city and state combination.

(
    city_revenue_df
    .write
    .mode("overwrite")
    .parquet(f"{GOLD_PATH}/city_revenue_metrics.parquet")
)#Parquet file creation in Gold layer.
#-------------------------------------
#  4️4.Monthly Sales Metrics
#-------------------------------------

monthly_sales_df = (
    silver_df
    .withColumn("year_month", date_format("order_date", "yyyy-MM"))
    .groupBy("year_month")
    .agg(
        round(sum("total_amount"), 2).alias("monthly_revenue"),
        count("transaction_id").alias("monthly_orders")
    )
)#Calculating the monthly sales metrics by extracting the year and month from the order date .
(
    monthly_sales_df
    .write
    .mode("overwrite")
    .parquet(f"{GOLD_PATH}/monthly_sales_metrics.parquet")
)#Parquet file creation.
#-------------------------------------
# 5.5️Top Products by Revenue
#-------------------------------------
top_products_df = (
    silver_df
    .groupBy("product_id")
    .agg(
        round(sum("total_amount"), 2).alias("revenue"),
        sum("quantity").alias("units_sold")
    )
    .orderBy(col("revenue").desc())
)#Calculating the top products by revenue.

(
    top_products_df
    .write
    .mode("overwrite")
    .parquet(f"{GOLD_PATH}/top_products.parquet")
)#Parquet file creation.
#-------------------------------------
# 6.Audit Metadata
#-------------------------------------
audit_df = spark.createDataFrame([
    Row(
        source_table="retail_sales_clean",
        source_rows=silver_df.count(),
        daily_sales_rows=daily_sales_df.count(),
        product_perf_rows=product_perf_df.count(),
        city_revenue_rows=city_revenue_df.count(),
        monthly_sales_rows=monthly_sales_df.count(),
        top_products_rows=top_products_df.count(),
        pipeline_status="SUCCESS",
        processed_time=str(datetime.now())
    )
])#Creating an audit DataFrame to track the number of rows processed in each of the Gold layer tables, along with the source table name and the processing timestamp.

(
    audit_df
    .coalesce(1)
    .write
    .mode("overwrite")
    .parquet(f"{GOLD_PATH}/audit_metadata.parquet")
)#Writing the audit metadata to the Gold layer as a Parquet file.

silver_df.unpersist()#Unpersisting the cached Silver DataFrame to free up memory resources.[as we used .cache()]
print("Gold layer created successfully")
spark.stop()

#INFO MapPartitionsRDD: Removing RDD 6 from persistence list
#INFO BlockManager: Removing RDD 6 search the meaning of these lines
