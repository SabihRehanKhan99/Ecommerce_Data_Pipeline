# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${catalog_name}.ecom_gold.market_basket_analysis_counts (
# MAGIC   analysis_date             DATE,   -- Date of the transactions included in the counts
# MAGIC   item_type                 STRING, -- 'product_id', 'category', 'brand'
# MAGIC   item_a_value              STRING, -- The actual value of item A (e.g., '100120466', 'appliances', 'samsung')
# MAGIC   item_b_value              STRING, -- The actual value of item B
# MAGIC   count_a_and_b             BIGINT, -- Number of transactions containing A and B
# MAGIC   count_a                   BIGINT, -- Number of transactions containing A
# MAGIC   count_b                   BIGINT  -- Number of transactions containing B
# MAGIC )
# MAGIC CLUSTER BY (analysis_date, item_type);
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT OVERWRITE TABLE ${catalog_name}.ecom_gold.market_basket_analysis_counts
# MAGIC WITH purchase_transactions_flattened AS (
# MAGIC   -- Create a unique transaction ID for each purchase event and flatten items
# MAGIC   SELECT
# MAGIC     CONCAT(user_session, '-', CAST(event_time AS STRING)) AS transaction_id,
# MAGIC     CAST(event_time AS DATE) AS transaction_date,
# MAGIC     product_id,
# MAGIC     category_L1 AS category,
# MAGIC     brand
# MAGIC   FROM
# MAGIC     ${catalog_name}.ecom_silver.marketplace_events_silver
# MAGIC   WHERE
# MAGIC     event_type = 'purchase'
# MAGIC   GROUP BY -- Ensure unique items per transaction
# MAGIC     CONCAT(user_session, '-', CAST(event_time AS STRING)),
# MAGIC     CAST(event_time AS DATE),
# MAGIC     product_id,
# MAGIC     category_L1,
# MAGIC     brand
# MAGIC ),
# MAGIC -- Prepare data for each item type
# MAGIC products_in_transactions AS (
# MAGIC   SELECT transaction_id, transaction_date, product_id AS item_value FROM purchase_transactions_flattened WHERE product_id IS NOT NULL
# MAGIC ),
# MAGIC categories_in_transactions AS (
# MAGIC   SELECT transaction_id, transaction_date, category AS item_value FROM purchase_transactions_flattened WHERE category IS NOT NULL
# MAGIC ),
# MAGIC brands_in_transactions AS (
# MAGIC   SELECT transaction_id, transaction_date, brand AS item_value FROM purchase_transactions_flattened WHERE brand IS NOT NULL
# MAGIC ),
# MAGIC -- NEW CTEs to pre-calculate count_a and count_b for each item type
# MAGIC product_item_counts AS (
# MAGIC   SELECT
# MAGIC     transaction_date,
# MAGIC     item_value,
# MAGIC     COUNT(DISTINCT transaction_id) AS item_transaction_count
# MAGIC   FROM
# MAGIC     products_in_transactions
# MAGIC   GROUP BY
# MAGIC     transaction_date,
# MAGIC     item_value
# MAGIC ),
# MAGIC category_item_counts AS (
# MAGIC   SELECT
# MAGIC     transaction_date,
# MAGIC     item_value,
# MAGIC     COUNT(DISTINCT transaction_id) AS item_transaction_count
# MAGIC   FROM
# MAGIC     categories_in_transactions
# MAGIC   GROUP BY
# MAGIC     transaction_date,
# MAGIC     item_value
# MAGIC ),
# MAGIC brand_item_counts AS (
# MAGIC   SELECT
# MAGIC     transaction_date,
# MAGIC     item_value,
# MAGIC     COUNT(DISTINCT transaction_id) AS item_transaction_count
# MAGIC   FROM
# MAGIC     brands_in_transactions
# MAGIC   GROUP BY
# MAGIC     transaction_date,
# MAGIC     item_value
# MAGIC ),
# MAGIC -- Refactored calculate_mba_counts
# MAGIC -- Refactored calculate_mba_counts
# MAGIC calculate_mba_counts AS (
# MAGIC   -- Product Pairs
# MAGIC   SELECT
# MAGIC     t1.transaction_date AS analysis_date, -- <--- Moved to first position
# MAGIC     'product_id' AS item_type,           -- <--- Moved to second position
# MAGIC     t1.item_value AS item_a_value,
# MAGIC     t2.item_value AS item_b_value,
# MAGIC     COUNT(DISTINCT t1.transaction_id) AS count_a_and_b,
# MAGIC     pic_a.item_transaction_count AS count_a,
# MAGIC     pic_b.item_transaction_count AS count_b
# MAGIC   FROM
# MAGIC     products_in_transactions t1
# MAGIC   JOIN
# MAGIC     products_in_transactions t2 ON t1.transaction_id = t2.transaction_id
# MAGIC   JOIN
# MAGIC     product_item_counts pic_a ON t1.item_value = pic_a.item_value AND t1.transaction_date = pic_a.transaction_date
# MAGIC   JOIN
# MAGIC     product_item_counts pic_b ON t2.item_value = pic_b.item_value AND t2.transaction_date = pic_b.transaction_date
# MAGIC   GROUP BY
# MAGIC     t1.transaction_date,
# MAGIC     t1.item_value,
# MAGIC     t2.item_value,
# MAGIC     pic_a.item_transaction_count,
# MAGIC     pic_b.item_transaction_count
# MAGIC
# MAGIC   UNION ALL
# MAGIC
# MAGIC   -- Category Pairs
# MAGIC   SELECT
# MAGIC     t1.transaction_date AS analysis_date, -- <--- Moved to first position
# MAGIC     'category' AS item_type,             -- <--- Moved to second position
# MAGIC     t1.item_value AS item_a_value,
# MAGIC     t2.item_value AS item_b_value,
# MAGIC     COUNT(DISTINCT t1.transaction_id) AS count_a_and_b,
# MAGIC     cic_a.item_transaction_count AS count_a,
# MAGIC     cic_b.item_transaction_count AS count_b
# MAGIC   FROM
# MAGIC     categories_in_transactions t1
# MAGIC   JOIN
# MAGIC     categories_in_transactions t2 ON t1.transaction_id = t2.transaction_id
# MAGIC   JOIN
# MAGIC     category_item_counts cic_a ON t1.item_value = cic_a.item_value AND t1.transaction_date = cic_a.transaction_date
# MAGIC   JOIN
# MAGIC     category_item_counts cic_b ON t2.item_value = cic_b.item_value AND t2.transaction_date = cic_b.transaction_date
# MAGIC   GROUP BY
# MAGIC     t1.transaction_date,
# MAGIC     t1.item_value,
# MAGIC     t2.item_value,
# MAGIC     cic_a.item_transaction_count,
# MAGIC     cic_b.item_transaction_count
# MAGIC
# MAGIC   UNION ALL
# MAGIC
# MAGIC   -- Brand Pairs
# MAGIC   SELECT
# MAGIC     t1.transaction_date AS analysis_date, -- <--- Moved to first position
# MAGIC     'brand' AS item_type,                -- <--- Moved to second position
# MAGIC     t1.item_value AS item_a_value,
# MAGIC     t2.item_value AS item_b_value,
# MAGIC     COUNT(DISTINCT t1.transaction_id) AS count_a_and_b,
# MAGIC     bic_a.item_transaction_count AS count_a,
# MAGIC     bic_b.item_transaction_count AS count_b
# MAGIC   FROM
# MAGIC     brands_in_transactions t1
# MAGIC   JOIN
# MAGIC     brands_in_transactions t2 ON t1.transaction_id = t2.transaction_id
# MAGIC   JOIN
# MAGIC     brand_item_counts bic_a ON t1.item_value = bic_a.item_value AND t1.transaction_date = bic_a.transaction_date
# MAGIC   JOIN
# MAGIC     brand_item_counts bic_b ON t2.item_value = bic_b.item_value AND t2.transaction_date = bic_b.transaction_date
# MAGIC   GROUP BY
# MAGIC     t1.transaction_date,
# MAGIC     t1.item_value,
# MAGIC     t2.item_value,
# MAGIC     bic_a.item_transaction_count,
# MAGIC     bic_b.item_transaction_count
# MAGIC )
# MAGIC SELECT * FROM calculate_mba_counts;
# MAGIC

# COMMAND ----------

