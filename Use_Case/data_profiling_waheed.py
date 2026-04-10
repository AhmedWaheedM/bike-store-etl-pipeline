# Databricks notebook source
"""
data_profiling_waheed.py — Data Profiling Notebook

Loads all source tables from the Landing Zone via metadata,
then runs the ``profile_data`` utility to inspect schema,
statistics, null counts, duplicates, and value distributions.
"""

# MAGIC %run ./config

# COMMAND ----------

# MAGIC %run ./utils

# COMMAND ----------

loaded_dataframes = load_and_cast_file_data(spark, metadata)

# COMMAND ----------

display(customers_df)

# COMMAND ----------

display(staffs_df)

# COMMAND ----------

display(order_items_df)

# COMMAND ----------

display(orders_df)

# COMMAND ----------

display(categories_df)

# COMMAND ----------

display(brands_df)

# COMMAND ----------

display(products_df)

# COMMAND ----------

display(staffs_df)

# COMMAND ----------

for name, df in loaded_dataframes.items():
    profile_data(df, df_name=name)