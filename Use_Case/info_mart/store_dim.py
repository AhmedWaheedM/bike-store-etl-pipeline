# Databricks notebook source
"""
store_dim.py — Store Dimension

Reads the cleansed stores table from Staging Layer 1
and writes it to the Info Mart as ``stores_dim``.
"""

# MAGIC %run ../config

# COMMAND ----------

stores_dim = spark.read.format("delta").load(f"{STAGE1_PATH}/stores")

# COMMAND ----------

display(stores_dim)

# COMMAND ----------

stores_dim.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(
    f"{INFOMART_PATH}/stores_dim"
)