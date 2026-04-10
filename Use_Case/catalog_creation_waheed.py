# Databricks notebook source
"""
catalog_creation_waheed.py — Unity / Hive Catalog Registration

Creates the ``waheed_db`` database and registers each Info Mart
Delta table as an external Spark SQL table so that downstream
notebooks can query them with plain SQL.
"""

# MAGIC %run ./config

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS waheed_db;

# COMMAND ----------

# MAGIC %sql
# MAGIC USE waheed_db;
# MAGIC CREATE TABLE IF NOT EXISTS customers_dim
# MAGIC USING DELTA
# MAGIC LOCATION "wasbs://dlbikestoreinfomart@adlsbikestoreinterns.blob.core.windows.net/Waheed/customers_dim"

# COMMAND ----------

# MAGIC %sql
# MAGIC USE waheed_db;
# MAGIC CREATE TABLE IF NOT EXISTS date_dim
# MAGIC USING DELTA
# MAGIC LOCATION "wasbs://dlbikestoreinfomart@adlsbikestoreinterns.blob.core.windows.net/Waheed/date_dim"

# COMMAND ----------

# MAGIC %sql
# MAGIC USE waheed_db;
# MAGIC CREATE TABLE IF NOT EXISTS order_fact
# MAGIC USING DELTA
# MAGIC LOCATION "wasbs://dlbikestoreinfomart@adlsbikestoreinterns.blob.core.windows.net/Waheed/order_fact"

# COMMAND ----------

# MAGIC %sql
# MAGIC USE waheed_db;
# MAGIC CREATE TABLE IF NOT EXISTS products_dim
# MAGIC USING DELTA
# MAGIC LOCATION "wasbs://dlbikestoreinfomart@adlsbikestoreinterns.blob.core.windows.net/Waheed/products_dim"

# COMMAND ----------

# MAGIC %sql
# MAGIC USE waheed_db;
# MAGIC CREATE TABLE IF NOT EXISTS staffs_dim
# MAGIC USING DELTA
# MAGIC LOCATION "wasbs://dlbikestoreinfomart@adlsbikestoreinterns.blob.core.windows.net/Waheed/staffs_dim"

# COMMAND ----------

# MAGIC %sql
# MAGIC USE waheed_db;
# MAGIC CREATE TABLE IF NOT EXISTS stores_dim
# MAGIC USING DELTA
# MAGIC LOCATION "wasbs://dlbikestoreinfomart@adlsbikestoreinterns.blob.core.windows.net/Waheed/stores_dim"

# COMMAND ----------

# MAGIC %sql
# MAGIC USE waheed_db;
# MAGIC CREATE TABLE IF NOT EXISTS status_dim
# MAGIC USING DELTA
# MAGIC LOCATION "wasbs://dlbikestoreinfomart@adlsbikestoreinterns.blob.core.windows.net/Waheed/status_dim"