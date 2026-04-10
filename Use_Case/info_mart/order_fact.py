# Databricks notebook source
"""
order_fact.py — Order Fact Table

Joins orders with order_items (line-item granularity) and maps
order_date, required_date, and shipped_date to surrogate date_id
keys from the date_lookup table.
"""

# MAGIC %run ../config

# COMMAND ----------

import pyspark.sql.functions as F

# COMMAND ----------

# MAGIC %run ./store_dim

# COMMAND ----------

order_df         = spark.read.format("delta").load(f"{STAGE1_PATH}/order")
order_items_df   = spark.read.format("delta").load(f"{STAGE1_PATH}/order_items")
date_lookup_df   = spark.read.format("delta").load(f"{STAGE2_PATH}/date_lookup")
stores           = spark.read.format("delta").load(f"{INFOMART_PATH}/stores_dim/")

# COMMAND ----------

# Drop audit columns and join orders ↔ order_items
order_df = order_df.drop("timestamp", "extraction_id")
order_fact_df = (
    order_df
    .join(order_items_df, order_df.order_id == order_items_df.order_id, "left")
    .drop(order_items_df.order_id)
    .drop("item_id", "source")
)

# COMMAND ----------

# Map order_date → order_date_id
order_fact_df = order_fact_df.join(
    date_lookup_df
        .withColumnRenamed("date", "order_date")
        .withColumnRenamed("date_id", "order_date_id"),
    "order_date", "left",
)

# Map required_date → required_date_id
order_fact_df = order_fact_df.join(
    date_lookup_df
        .withColumnRenamed("date", "required_date")
        .withColumnRenamed("date_id", "required_date_id"),
    "required_date", "left",
)

# Map shipped_date → shipped_date_id
order_fact_df = order_fact_df.join(
    date_lookup_df
        .withColumnRenamed("date", "shipped_date")
        .withColumnRenamed("date_id", "shipped_date_id"),
    "shipped_date", "left",
)

order_fact_df = order_fact_df.drop("order_date", "required_date", "shipped_date")

# COMMAND ----------

display(order_fact_df)

# COMMAND ----------

order_fact_df.printSchema()

# COMMAND ----------

# Quick validation: join with stores to verify store_name
displayed_df = (
    order_fact_df.alias("a")
    .join(stores.alias("b"), on=F.col("a.store_id") == F.col("b.store_id"), how="inner")
    .select(["a.*", "b.store_name"])
)
display(displayed_df)

# COMMAND ----------

order_fact_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(
    f"{INFOMART_PATH}/order_fact"
)