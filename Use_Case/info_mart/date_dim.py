# Databricks notebook source
"""
date_dim.py — Date Dimension

Reads the date lookup from Staging Layer 2 and enriches it with
calendar attributes: year, quarter, month, week, day, day_of_week,
and day_name for time-series analysis and BI slicing.
"""

# MAGIC %run ../config

# COMMAND ----------

from pyspark.sql.functions import col, year, quarter, month, weekofyear, dayofmonth, dayofweek, date_format

# COMMAND ----------

date_dim = spark.read.format("delta").load(f"{STAGE2_PATH}/date_lookup")

# COMMAND ----------

date_dim = (
    date_dim
    .withColumn("year",        year(col("date")))
    .withColumn("quarter",     quarter(col("date")))
    .withColumn("month",       month(col("date")))
    .withColumn("week",        weekofyear(col("date")))
    .withColumn("day",         dayofmonth(col("date")))
    .withColumn("day_of_week", dayofweek(col("date")))
    .withColumn("day_name",    date_format(col("date"), "EEEE"))
)

# COMMAND ----------

display(date_dim)

# COMMAND ----------

date_dim.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(
    f"{INFOMART_PATH}/date_dim"
)