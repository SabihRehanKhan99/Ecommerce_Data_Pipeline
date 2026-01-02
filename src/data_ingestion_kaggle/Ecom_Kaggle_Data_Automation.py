# Databricks notebook source


# 1. Widget for file name
dbutils.widgets.text("fileName", "")
dbutils.widgets.text("catalog_name", "")
file_name = dbutils.widgets.get("fileName")
catalog_name = dbutils.widgets.get("catalog_name")
if not file_name:
    raise ValueError("You must supply a fileName via widget")

# 2. Build paths
url   = f"https://data.rees46.com/datasets/marketplace/{file_name}"
local = f"/tmp/{file_name}"
dbfs  = f"dbfs:/tmp/marketplace/{file_name}"

# 3. Download and copy
import urllib.request
urllib.request.urlretrieve(url, local)
dbutils.fs.mkdirs("dbfs:/tmp/marketplace")
dbutils.fs.cp(f"file:{local}", dbfs, True)


from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType

schema = StructType([
    StructField("event_time", TimestampType(), True),
    StructField("event_type", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("category_id", StringType(), True),
    StructField("category_code", StringType(), True),
    StructField("brand", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("user_id", StringType(), True),
    StructField("user_session", StringType(), True),
])
# 4. Load into DataFrame
from pyspark.sql.functions import col
df = (spark.read
            .format("csv")
            .option("header", True)
            .option("compression", "gzip")
            .schema(schema)
            .load(dbfs))
df.printSchema()

# 5. Filter valid event types
valid_types = ["view", "cart", "remove_from_cart", "purchase"]
df_valid = df.filter(col("event_type").isin(valid_types))

# 6. Save to bronze table
# df_valid.write.format("delta").mode("overwrite").saveAsTable("ecom_bronze.marketplace_events")
df_valid.write.format("delta").mode("append").insertInto(f"{catalog_name}.ecom_bronze.marketplace_events")

# 7. Display sample
display(df_valid.limit(5))


# COMMAND ----------


