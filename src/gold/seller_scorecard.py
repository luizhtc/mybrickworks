# Databricks notebook source
from utils import read_from_silver
from pyspark.sql.functions import col, countDistinct, sum as _sum, count as _count

# COMMAND ----------

orders = read_from_silver("orders")
order_items = read_from_silver("order_items")
reviews = read_from_silver("reviews")

# COMMAND ----------

valid_sellers =\
orders.filter(col("order_status") == "delivered").join(
    order_items,
    on="order_id",
    how="left"
).groupBy("seller_id").agg(
    countDistinct(col("order_id")).alias("total_delivered_orders")
).filter(col("total_delivered_orders") >= 20).select("seller_id").distinct()

# COMMAND ----------

order_items.groupby("seller_id","seller_state").agg(
    countDistinct(col("order_id")).alias("total_orders"),
    _sum(col("price")).alias("total_revenue")
).join(
    valid_sellers,
    on="seller_id",
    how="inner"
).display()

# COMMAND ----------

reviews.display()

# COMMAND ----------

order_seller = order_items.select("order_id","seller_id").distinct()

# COMMAND ----------

order_seller.join(
    orders,
    on="order_id",
    how="inner"
).display()

# COMMAND ----------

orders.join(
    reviews,
    on="order_id",
    how="inner"
).display()
