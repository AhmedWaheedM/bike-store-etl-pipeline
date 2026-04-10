# Databricks notebook source
"""
products_dim.py — Products Dimension

Joins products with brands and categories from Staging Layer 1
to create a denormalised products dimension for the Info Mart.
"""

# MAGIC %run ../config

# COMMAND ----------

products_df    = spark.read.format("delta").load(f"{STAGE1_PATH}/products")
brands_df      = spark.read.format("delta").load(f"{STAGE1_PATH}/brands")
categories_df  = spark.read.format("delta").load(f"{STAGE1_PATH}/categories")

# COMMAND ----------

# Drop audit columns before joining
products_df = products_df.drop("timestamp", "extraction_id")

# Join brands
products_dim = (
    products_df
    .join(brands_df, products_df.brand_id == brands_df.brand_id, "left")
    .drop("timestamp", "extraction_id")
)

# Join categories
products_dim = (
    products_dim
    .join(categories_df, products_dim.category_id == categories_df.category_id, "left")
    .drop("brand_id", "category_id", "source")
)

# COMMAND ----------

display(products_dim)

# COMMAND ----------

products_dim.printSchema()

# COMMAND ----------

products_dim.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(
    f"{INFOMART_PATH}/products_dim"
)