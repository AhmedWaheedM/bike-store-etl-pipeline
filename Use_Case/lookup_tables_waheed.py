# Databricks notebook source
"""
lookup_tables_waheed.py — Lookup / Reference Table Creation

Creates two surrogate-key lookup tables in Staging Layer 2:
  • date_lookup   – one row per day from 2016-01-01 to 2019-12-31
  • shipping_lookup – maps status_id → human-readable order status
"""

# MAGIC %run ./config

# COMMAND ----------

from pyspark.sql.functions import row_number
from pyspark.sql import Row
from pyspark.sql.window import Window

# COMMAND ----------

# --- Date Lookup ---
date_df = spark.sql(
    "SELECT explode(sequence(to_date('2016-01-01'), to_date('2019-12-31'), interval 1 day)) AS date"
)
date_df = date_df.withColumn(
    "date_id", row_number().over(Window.orderBy("date")).astype("int")
)

date_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(
    f"{STAGE2_PATH}/date_lookup"
)

# COMMAND ----------

# --- Shipping Status Lookup ---
shipping_data = [
    Row(status_id=1, status="Pending"),
    Row(status_id=2, status="Processing"),
    Row(status_id=3, status="Rejected"),
    Row(status_id=4, status="Completed"),
]
shipping_df = spark.createDataFrame(shipping_data)

shipping_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(
    f"{STAGE2_PATH}/shipping_lookup"
)

# COMMAND ----------

display(shipping_df)
