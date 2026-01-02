# Databricks notebook source
# MAGIC %sql
# MAGIC -- Create the gold table if it doesn't exist
# MAGIC CREATE TABLE IF NOT EXISTS ${catalog_name}.ecom_gold.engagement_metrics (
# MAGIC     date DATE,
# MAGIC     total_sessions BIGINT,
# MAGIC     avg_session_duration DOUBLE,
# MAGIC     avg_views_per_session DOUBLE,
# MAGIC     single_view_sessions BIGINT,
# MAGIC     avg_revenue_per_session DOUBLE,
# MAGIC     avg_session_engagement_score DOUBLE,
# MAGIC     new_sessions BIGINT,
# MAGIC     new_single_view_sessions BIGINT,
# MAGIC     total_engagement_score DOUBLE,
# MAGIC     total_vps DOUBLE,
# MAGIC     total_rps DOUBLE,
# MAGIC     total_sd DOUBLE
# MAGIC )
# MAGIC CLUSTER BY(date); 
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC INSERT OVERWRITE TABLE ${catalog_name}.ecom_gold.engagement_metrics
# MAGIC
# MAGIC WITH session_events AS (
# MAGIC     -- Extract date, session information, event type, and price
# MAGIC     SELECT
# MAGIC         CAST(event_time AS DATE) AS event_date,
# MAGIC         user_session,
# MAGIC         event_time,
# MAGIC         event_type,
# MAGIC         price
# MAGIC     FROM ${catalog_name}.ecom_silver.marketplace_events_silver
# MAGIC ),
# MAGIC session_metrics AS (
# MAGIC     -- Calculate metrics for each individual session
# MAGIC     SELECT
# MAGIC         event_date,
# MAGIC         user_session,
# MAGIC         MIN(event_time) AS session_start,
# MAGIC         MAX(event_time) AS session_end,
# MAGIC         -- Calculate session duration in minutes
# MAGIC         (UNIX_TIMESTAMP(MAX(event_time)) - UNIX_TIMESTAMP(MIN(event_time))) / 60 AS session_duration_minutes,
# MAGIC         -- Calculate total revenue for the session (only from 'purchase' events)
# MAGIC         SUM(CASE WHEN event_type = 'purchase' THEN price ELSE 0 END) AS total_revenue_in_session,
# MAGIC         -- Count the number of specific event types within each session
# MAGIC         COUNT(CASE WHEN event_type = 'view' THEN 1 END) AS views_in_session,
# MAGIC         COUNT(CASE WHEN event_type = 'add_to_cart' THEN 1 END) AS add_to_carts_in_session,
# MAGIC         COUNT(CASE WHEN event_type = 'checkout' THEN 1 END) AS checkouts_in_session
# MAGIC     FROM session_events
# MAGIC     GROUP BY event_date, user_session
# MAGIC ),
# MAGIC first_session_occurrence AS (
# MAGIC     -- Determine the very first date each user_session appeared in the entire dataset
# MAGIC     SELECT
# MAGIC         user_session,
# MAGIC         MIN(CAST(event_time AS DATE)) AS first_appearance_date
# MAGIC     FROM ${catalog_name}.ecom_silver.marketplace_events_silver
# MAGIC     GROUP BY user_session
# MAGIC )
# MAGIC -- Aggregate the session-level metrics by date to get the final engagement metrics
# MAGIC SELECT
# MAGIC     sm.event_date AS date,
# MAGIC     COUNT(DISTINCT sm.user_session) AS total_sessions,
# MAGIC     AVG(sm.session_duration_minutes) AS avg_session_duration,
# MAGIC     AVG(sm.views_in_session) AS avg_views_per_session,
# MAGIC     COUNT(CASE WHEN sm.views_in_session = 1 THEN 1 END) AS single_view_sessions,
# MAGIC     AVG(sm.total_revenue_in_session) AS avg_revenue_per_session,
# MAGIC     -- Calculate the average session engagement score
# MAGIC     AVG(
# MAGIC         sm.views_in_session * 1 +
# MAGIC         sm.add_to_carts_in_session * 2 +
# MAGIC         sm.checkouts_in_session * 3 +
# MAGIC         sm.session_duration_minutes * 4
# MAGIC     ) AS avg_session_engagement_score,
# MAGIC     COUNT(CASE WHEN sm.event_date = fso.first_appearance_date THEN 1 END) AS new_sessions,
# MAGIC     -- Calculate new_sessions: single-view sessions that are also the first appearance of that user_session
# MAGIC     COUNT(CASE WHEN sm.views_in_session = 1 AND sm.event_date = fso.first_appearance_date THEN 1 END) AS new_single_view_sessions,
# MAGIC     (AVG(
# MAGIC         sm.views_in_session * 1 +
# MAGIC         sm.add_to_carts_in_session * 2 +
# MAGIC         sm.checkouts_in_session * 3 +
# MAGIC         sm.session_duration_minutes * 4
# MAGIC     ) * COUNT(DISTINCT sm.user_session)) AS total_engagement_score,
# MAGIC     (AVG(sm.views_in_session) * COUNT(DISTINCT sm.user_session)) as total_vps,
# MAGIC     (AVG(sm.total_revenue_in_session) * COUNT(DISTINCT sm.user_session)) as total_rps,
# MAGIC     (AVG(sm.session_duration_minutes) * COUNT(DISTINCT sm.user_session)) as total_sd
# MAGIC FROM session_metrics sm
# MAGIC JOIN first_session_occurrence fso
# MAGIC     ON sm.user_session = fso.user_session
# MAGIC GROUP BY sm.event_date
# MAGIC ORDER BY sm.event_date;