# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${catalog_name}.ecom_gold.user_purchase_history (
# MAGIC   user_id                   BIGINT,
# MAGIC   purchase_event_time       TIMESTAMP, -- Exact timestamp of the purchase event
# MAGIC   purchase_date             DATE,      -- Date of the purchase
# MAGIC   purchase_revenue          DOUBLE,    -- Revenue from this specific purchase event
# MAGIC   purchase_number           BIGINT     -- This user's 1st, 2nd, 3rd, etc. purchase
# MAGIC )
# MAGIC CLUSTER BY(purchase_event_time, purchase_date);

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC INSERT OVERWRITE TABLE ${catalog_name}.ecom_gold.user_purchase_history
# MAGIC WITH user_purchase_events AS (
# MAGIC   SELECT
# MAGIC     user_id,
# MAGIC     event_time,
# MAGIC     price
# MAGIC   FROM
# MAGIC     ${catalog_name}.ecom_silver.marketplace_events_silver
# MAGIC   WHERE
# MAGIC     event_type = 'purchase'
# MAGIC )
# MAGIC SELECT
# MAGIC   user_id,
# MAGIC   event_time AS purchase_event_time,
# MAGIC   CAST(event_time AS DATE) AS purchase_date,
# MAGIC   price AS purchase_revenue,
# MAGIC   ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY event_time ASC) AS purchase_number
# MAGIC FROM
# MAGIC   user_purchase_events;
# MAGIC