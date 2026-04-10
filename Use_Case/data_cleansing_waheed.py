# Databricks notebook source
"""
data_cleansing_waheed.py — Data Cleansing Notebook

Applies the full cleansing pipeline to every source table:
  1. String normalisation (trim, lower-case, regex cleanup)
  2. Duplicate removal  (per metadata unique-flag rules)
  3. Null handling       (drop or fill per metadata nullable-flag)
  4. Outlier treatment   (Z-score on order_items numeric columns)
  5. Zip-code validation (pgeocode US validation on stores)
  6. Primary-key validation
  7. Column renaming     (prefix shared column names with table name)

Cleaned DataFrames are written as Delta tables to Staging Layer 1.
"""

# MAGIC %run ./config

# COMMAND ----------

# MAGIC %run ./utils

# COMMAND ----------

# MAGIC %pip install pgeocode

# COMMAND ----------

from pyspark.sql import SparkSession, DataFrame, Row
from pyspark.sql.functions import col, sum, count
from pyspark.sql.types import StringType, IntegerType, FloatType, DoubleType, LongType
from pyspark.sql import functions as F
from delta.tables import *

# COMMAND ----------

load_and_cast_file_data(spark, metadata)

# COMMAND ----------

dataframes = {}
for df_name, df in [(df_name, globals()[df_name]) for df_name in dir() if df_name.endswith("_df")]:
    dataframes[df_name[:-3]] = df

# COMMAND ----------

normalize_strings(dataframes=dataframes, metadata_df=metadata)

# COMMAND ----------

# --- Customers ---
customers_df = handle_duplicates(customers_df, metadata_df=metadata, table_name="customers")
customers_df = handle_nulls(customers_df, metadata_df=metadata, table_name="customers")

# COMMAND ----------

# --- Categories ---
categories_df = handle_duplicates(categories_df, metadata_df=metadata, table_name="categories")
categories_df = handle_nulls(categories_df, metadata_df=metadata, table_name="categories")

# COMMAND ----------

# --- Staff ---
staffs_df = handle_nulls(staffs_df, metadata_df=metadata, table_name="staffs")
staffs_df = handle_duplicates(staffs_df, metadata_df=metadata, table_name="staffs")

# COMMAND ----------

# --- Orders ---
order_df = handle_nulls(order_df, metadata_df=metadata, table_name="order")
order_df = handle_duplicates(order_df, metadata_df=metadata, table_name="order")

# COMMAND ----------

# --- Order Items (with outlier treatment) ---
order_items_df = handle_nulls(order_items_df, metadata_df=metadata, table_name="order_items")
order_items_df = handle_duplicates(order_items_df, metadata_df=metadata, table_name="order_items")
order_items_df = handle_outliers(order_items_df, column="list_price",  method="zscore", strategy="remove")
order_items_df = handle_outliers(order_items_df, column="quantity",    method="zscore", strategy="remove")
order_items_df = handle_outliers(order_items_df, column="discount",    method="zscore", strategy="remove")

# COMMAND ----------

# --- Brands ---
brands_df = handle_duplicates(brands_df, metadata_df=metadata, table_name="brands")
brands_df = handle_nulls(brands_df, metadata_df=metadata, table_name="brands")

# COMMAND ----------

# --- Products ---
products_df = handle_duplicates(products_df, metadata_df=metadata, table_name="products")
products_df = handle_nulls(products_df, metadata_df=metadata, table_name="products")

# COMMAND ----------

# --- Stores (with zip-code validation) ---
stores_df = handle_duplicates(stores_df, metadata_df=metadata, table_name="stores")
stores_df = handle_nulls(stores_df, metadata_df=metadata, table_name="stores")
stores_df = handle_zip_codes(stores_df, column_name="zip_code")

# COMMAND ----------

validate_primary_keys(dataframes=dataframes, metadata_df=metadata)

# COMMAND ----------

# Rebuild dataframes dict after cleansing
dataframes = {}
for df_name, df in [(df_name, globals()[df_name]) for df_name in dir() if df_name.endswith("_df")]:
    dataframes[df_name[:-3]] = df

# COMMAND ----------

# Rename shared columns to avoid ambiguity in downstream joins
columns_to_rename = [
    "first_name", "last_name", "phone", "email",
    "street", "city", "state", "zip_code",
]

for table_name, df in dataframes.items():
    for col_name in df.columns:
        if col_name in columns_to_rename:
            new_col_name = f"{table_name}_{col_name}"
            df = df.withColumnRenamed(col_name, new_col_name)
            dataframes[table_name] = df

# COMMAND ----------

# Write cleaned DataFrames to Staging Layer 1 as Delta tables
for table_name, df in dataframes.items():
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(
        f"{STAGE1_PATH}/{table_name}"
    )