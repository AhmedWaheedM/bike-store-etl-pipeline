# Databricks notebook source
bikes_account_key = dbutils.secrets.get(scope = "bikes-scope", key = "account-key")
spark.conf.set(
    "fs.azure.account.key.adlsbikestoreinterns.blob.core.windows.net",
    bikes_account_key
)
spark.conf.set("fs.azure.account.key.adlsbikestoreinterns.blob.core.windows.net", "2A7Fbw9UkJ7R03r/x+uw6/JNClb4b73dpL+KGsIVfAOpJfRVBjH1rjMC2xLTj4uVCR/j8qgXfabp+AStWjlnNg==")
dbutils.secrets.listScopes()
dbutils.secrets.list(scope = "bikes-scope")
bikes_account_key = dbutils.secrets.get(scope = "bikes-scope", key = "account-key")
spark.conf.set(
    "fs.azure.account.key.adlsbikestoreinterns.blob.core.windows.net",
    bikes_account_key
)

# COMMAND ----------

MAGIC %sql
MAGIC drop table if exists waheed_db.sales_datamart

# COMMAND ----------

MAGIC %sql
MAGIC USE waheed_db;
MAGIC CREATE TABLE sales_datamart AS
MAGIC (
MAGIC SELECT 
MAGIC     f.* EXCEPT (store_id, staff_id,product_id, extraction_id, timestamp, order_date_id, required_date_id, shipped_date_id,list_price),
MAGIC     f.list_price as order_list_price,
MAGIC     pd.* EXCEPT ( extraction_id, timestamp,list_price),
MAGIC     pd.list_price as product_list_price,
MAGIC     sd.* EXCEPT (store_id, extraction_id, source, timestamp),
MAGIC     stfd.* EXCEPT (staff_id, store_id, extraction_id, source, timestamp),
MAGIC     cd.* EXCEPT (customer_id, extraction_id, source, timestamp),
MAGIC     ssd.status,
MAGIC     dd.date as ordered_date,
MAGIC     dd2.date as required_date,
MAGIC     dd3.date as shipped_date,
MAGIC     -- Calculated columns
MAGIC     f.list_price * f.quantity AS revenue_per_product,
MAGIC     DATEDIFF(dd3.date, dd2.date) AS delay_time,
MAGIC     f.list_price * (1 - f.discount) AS price_after_discount,
MAGIC     (f.list_price * (1 - f.discount )) * f.quantity AS revenue_after_discount,
MAGIC     DATEDIFF(dd3.date, dd.date) AS days_to_ship,
MAGIC     CASE WHEN dd3.date IS NOT NULL THEN 1 ELSE 0 END AS fulfilled_order_flag,
MAGIC     CASE WHEN f.list_price * f.quantity > 1000 THEN 1 ELSE 0 END AS high_value_order_flag,
MAGIC     CASE WHEN f.customer_id IN (
MAGIC     SELECT customer_id FROM waheed_db.order_fact GROUP BY customer_id HAVING COUNT(order_id) > 5
MAGIC     ) THEN 1 ELSE 0 END AS frequent_customer_flag
MAGIC
MAGIC
MAGIC FROM 
MAGIC     waheed_db.order_fact f
MAGIC FULL OUTER JOIN 
MAGIC     waheed_db.products_dim pd ON f.product_id = pd.product_id
MAGIC LEFT JOIN 
MAGIC     waheed_db.stores_dim sd ON f.store_id = sd.store_id
MAGIC LEFT JOIN 
MAGIC     waheed_db.staffs_dim stfd ON f.staff_id = stfd.staff_id
MAGIC LEFT JOIN 
MAGIC     waheed_db.customers_dim cd ON f.customer_id = cd.customer_id
MAGIC LEFT JOIN 
MAGIC     waheed_db.status_dim ssd ON f.order_status = ssd.status_id
MAGIC LEFT JOIN
MAGIC     waheed_db.date_dim dd ON f.order_date_id = dd.date_id
MAGIC LEFT JOIN
MAGIC     waheed_db.date_dim dd2 ON f.required_date_id = dd2.date_id 
MAGIC LEFT JOIN
MAGIC     waheed_db.date_dim dd3 ON f.shipped_date_id = dd3.date_id
MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from waheed_db.sales_datamart
# MAGIC

# COMMAND ----------

spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

# COMMAND ----------

MAGIC %sql
MAGIC CREATE TABLE IF NOT EXISTS waheed_db.sales_datamart_waheed
MAGIC USING DELTA
MAGIC LOCATION 'wasbs://waheeddatamart@adlsbikestoreinterns.blob.core.windows.net/sales_datamart';
MAGIC
MAGIC -- Insert data from the existing table into the external table
MAGIC INSERT OVERWRITE TABLE waheed_db.sales_datamart_waheed
MAGIC SELECT * FROM waheed_db.sales_datamart;