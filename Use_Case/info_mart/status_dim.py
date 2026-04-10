# Databricks notebook source
"""
status_dim.py — Order Status Dimension

Reads the shipping/status lookup from Staging Layer 2
and writes it to the Info Mart as ``status_dim``.
"""

# MAGIC %run ../config

# COMMAND ----------

status_dim = spark.read.format("delta").load(f"{STAGE2_PATH}/shipping_lookup")

# COMMAND ----------

status_dim.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(
    f"{INFOMART_PATH}/status_dim"
)