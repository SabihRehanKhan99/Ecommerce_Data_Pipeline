# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS ${catalog_name}.ecom_silver;
# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${catalog_name}.ecom_silver.marketplace_events_silver (
# MAGIC   event_time     TIMESTAMP   NOT NULL,
# MAGIC   event_type     STRING      NOT NULL,
# MAGIC   product_id     STRING      NOT NULL,
# MAGIC   category_id    STRING      NOT NULL,
# MAGIC   brand          STRING,
# MAGIC   price          DOUBLE      NOT NULL,
# MAGIC   user_id        STRING      NOT NULL,
# MAGIC   user_session   STRING      NOT NULL,
# MAGIC   category_L1    STRING,
# MAGIC   category_L2    STRING,
# MAGIC   category_L3    STRING
# MAGIC )
# MAGIC CLUSTER BY (event_time,event_type,category_L1,brand)
# MAGIC


# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE ${catalog_name}.ecom_silver.marketplace_events_silver
# MAGIC ALTER COLUMN category_id DROP NOT NULL;
# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT OVERWRITE ${catalog_name}.ecom_silver.marketplace_events_silver
# MAGIC SELECT
# MAGIC   CAST(event_time AS TIMESTAMP)           AS event_time,
# MAGIC   CAST(event_type AS STRING)              AS event_type,
# MAGIC   CAST(product_id AS STRING)              AS product_id,
# MAGIC   CAST(category_id AS STRING)             AS category_id,
# MAGIC   CAST(brand AS STRING)                   AS brand,
# MAGIC   CAST(price AS DOUBLE)                   AS price,
# MAGIC   CAST(user_id AS STRING)                 AS user_id,
# MAGIC   CAST(user_session AS STRING)            AS user_session,
# MAGIC   element_at(split(category_code, '\\.'), 1) AS category_L1,
# MAGIC   element_at(split(category_code, '\\.'), 2) AS category_L2,
# MAGIC   element_at(split(category_code, '\\.'), 3) AS category_L3
# MAGIC FROM ${catalog_name}.ecom_bronze.marketplace_events
# MAGIC WHERE
# MAGIC   event_time IS NOT NULL AND
# MAGIC   event_type IS NOT NULL AND
# MAGIC   product_id IS NOT NULL AND
# MAGIC   price IS NOT NULL AND
# MAGIC   user_id IS NOT NULL AND
# MAGIC   user_session IS NOT NULL;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from ${catalog_name}.ecom_silver.marketplace_events_silver limit 10

# COMMAND ----------


