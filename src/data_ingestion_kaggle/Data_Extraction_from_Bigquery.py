# Databricks notebook source
# DBTITLE 1,Working code for all bigquery tables
from pyspark.sql import SparkSession
from datetime import datetime, timedelta

dbutils.widgets.text("catalog_name", "")

catalog_name = dbutils.widgets.get("catalog_name")

# Initialize SparkSession with proper BigQuery configuration
spark = SparkSession.builder \
    .appName("BigQueryReadExample") \
    .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
    .getOrCreate()

# Set BigQuery configurations
spark.conf.set("parentProject", "gatest-466013")
spark.conf.set("credentialsFile", "/dbfs/FileStore/tables/gc_creds.json")

# Function to generate table names for the given date range
def generate_table_names(start_date, end_date):
    table_names = []
    current_date = start_date
    while current_date <= end_date:
        table_name = f"bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_{current_date.strftime('%Y%m%d')}"
        table_names.append(table_name)
        current_date += timedelta(days=1)
    return table_names

# Function to read multiple tables and union them
def read_multiple_tables_dynamic():
    start_date = datetime(2020, 11, 1)
    end_date = datetime(2021, 1, 31)
    table_names = generate_table_names(start_date, end_date)
    
    dfs = []
    for table in table_names:
        try:
            df_temp = spark.read.format("bigquery") \
                .option("table", table) \
                .option("parentProject", "gatest-466013") \
                .load()
            dfs.append(df_temp)
            print(f"Successfully read {table}")
        except Exception as e:
            print(f"Failed to read {table}: {e}")
    
    if dfs:
        # Union all dataframes
        df_combined = dfs[0]
        for df_temp in dfs[1:]:
            df_combined = df_combined.union(df_temp)
        
        df_combined.createOrReplaceTempView("ga4_events_combined")
        df_combined.show(5)
        return df_combined
    else:
        print("No tables were successfully read")
        return None

# Call the function to read all tables using the dynamic approach
df_combined = read_multiple_tables_dynamic()


# COMMAND ----------

# DBTITLE 1,transforming to bronze
from pyspark.sql import functions as F

# Create transformed DataFrame
transformed_df = df_combined \
    .withColumn("event_time", F.from_unixtime(F.col("event_timestamp") / 1_000_000).cast("timestamp")) \
    .withColumn("event_type", F.col("event_name")) \
    .withColumn("user_session", F.col("stream_id")) \
    .withColumn("user_id", F.col("user_pseudo_id").cast("int"))  # If not available, hash "user_pseudo_id"

# Flatten items array (explode) to generate one row per item
flattened_df = transformed_df.withColumn("item", F.explode_outer("items")) \
    .select(
        "event_time",
        "event_type",
        F.col("item.item_id").cast("string").alias("product_id"),
        F.lit(None).cast("string").alias("category_id"),  # Not directly available
        F.col("item.item_category").cast("string").alias("category_code"),
        F.col("item.item_brand").alias("brand"),
        F.col("item.price").cast("double").alias("price"),
        F.col("user_id").cast("string"),
        F.col("user_session").cast("string")
    )


# COMMAND ----------

from pyspark.sql.functions import when, col

normalized_df = flattened_df.withColumn(
    "event_type",
    when(col("event_type") == "purchase", "purchase")
    .when(col("event_type").isin(
        "add_to_cart", "remove_from_cart", "begin_checkout", "add_shipping_info", "add_payment_info"
    ), "cart")
    .when(col("event_type").isin(
        "view_item", "view_item_list", "view_promotion", "select_item", "select_promotion", 
        "page_view", "view_search_results", "click", "scroll", "user_engagement", "session_start", "first_visit"
    ), "view")
    .otherwise("other")
)

# COMMAND ----------

normalized_df.write.format("delta").mode("overwrite").saveAsTable(f"{catalog_name}.ecom_bronze.marketplace_events")

# COMMAND ----------

