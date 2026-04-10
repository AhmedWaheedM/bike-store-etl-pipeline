# Databricks notebook source
"""
staff_dim.py — Staff Dimension

Reads the cleansed staffs table from Staging Layer 1
and writes it to the Info Mart as ``staffs_dim``.
"""

# MAGIC %run ../config

# COMMAND ----------

staff_dim = spark.read.format("delta").load(f"{STAGE1_PATH}/staffs")

# COMMAND ----------

display(staff_dim)

# COMMAND ----------

staff_dim.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(
    f"{INFOMART_PATH}/staffs_dim"
)