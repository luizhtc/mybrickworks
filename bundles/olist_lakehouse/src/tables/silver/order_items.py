# Databricks notebook source
from pyspark.sql.functions import col
from bundles.olist_lakehouse.src.tables.silver.utils import read_from_bronze

# COMMAND ----------

order_items = read_from_bronze("tbl_olist_order_items_dataset")
sellers = read_from_bronze("tbl_olist_sellers_dataset")

# COMMAND ----------

order_items_join = order_items.join(sellers, on="seller_id", how="left")

# COMMAND ----------

order_items_transformed = order_items_join.select(
    "order_id",
    col("order_item_id").cast("int"),
    "product_id",
    col("price").cast("double"),
    col("freight_value").cast("double"),
    "seller_id",
    "seller_city",
    "seller_state",
    "seller_zip_code_prefix",
)

# COMMAND ----------

(
    order_items_transformed.write.format("delta")
    .mode("overwrite")
    .saveAsTable("`cat_olist`.`sch_silver`.`order_items`")
)
