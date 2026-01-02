# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS ${catalog_name}.ecom_bronze;
# MAGIC
# MAGIC CREATE OR REPLACE TABLE ${catalog_name}.ecom_bronze.marketplace_events (
# MAGIC   event_time TIMESTAMP,
# MAGIC   event_type STRING,
# MAGIC   product_id    STRING,
# MAGIC   category_id    STRING,
# MAGIC   category_code      STRING,
# MAGIC   brand          STRING,
# MAGIC   price          DOUBLE,
# MAGIC   user_id        STRING,
# MAGIC   user_session   STRING
# MAGIC   
# MAGIC )
# MAGIC USING DELTA
# MAGIC

# COMMAND ----------

