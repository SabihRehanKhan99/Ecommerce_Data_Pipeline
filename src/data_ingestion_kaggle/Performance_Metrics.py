# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${catalog_name}.ecom_gold.performance_metrics (
# MAGIC   date               DATE,
# MAGIC   brand              STRING,
# MAGIC   category           STRING,
# MAGIC   product_id         STRING,
# MAGIC   views              BIGINT,
# MAGIC   add_to_cart        BIGINT,
# MAGIC   checkouts          BIGINT,
# MAGIC   revenue            DOUBLE,
# MAGIC   orders             BIGINT
# MAGIC )
# MAGIC CLUSTER BY (date, brand, category, product_id);

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT OVERWRITE TABLE ${catalog_name}.ecom_gold.performance_metrics
# MAGIC (date,brand,category,product_id,views,add_to_cart,checkouts,revenue,orders)
# MAGIC WITH base AS (
# MAGIC     SELECT
# MAGIC       CAST(event_time AS DATE)            AS date,
# MAGIC       brand,
# MAGIC       category_L1                         AS category,
# MAGIC       product_id,
# MAGIC       sum(CASE WHEN event_type='view' THEN 1 ELSE NULL END) AS views,
# MAGIC       sum(CASE WHEN event_type='purchase' THEN 1 ELSE NULL END) AS checkouts,
# MAGIC       sum(CASE WHEN event_type='cart' THEN 1 ELSE NULL END) AS add_to_cart,
# MAGIC       SUM(CASE WHEN event_type='purchase' THEN price ELSE 0 END) AS revenue,
# MAGIC       count(distinct CASE WHEN event_type='purchase' THEN user_session ELSE 0 END) AS orders
# MAGIC     FROM ${catalog_name}.ecom_silver.marketplace_events_silver
# MAGIC     GROUP BY date, brand, category, product_id
# MAGIC   )
# MAGIC
# MAGIC   SELECT
# MAGIC     date,
# MAGIC     brand,
# MAGIC     category,
# MAGIC     product_id,
# MAGIC     views,
# MAGIC     add_to_cart,
# MAGIC     checkouts,
# MAGIC     revenue,
# MAGIC     orders
# MAGIC
# MAGIC   FROM base order by date 

# COMMAND ----------


