# Databricks notebook source
"""
customer_dim.py — Customer Dimension

Reads the cleansed customers table from Staging Layer 1
and writes it to the Info Mart as ``customers_dim``.
"""

# MAGIC %run ../config

# COMMAND ----------

customers_dim = spark.read.format("delta").load(f"{STAGE1_PATH}/customers")

# COMMAND ----------

display(customers_dim)

# COMMAND ----------

customers_dim.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(
    f"{INFOMART_PATH}/customers_dim"
)