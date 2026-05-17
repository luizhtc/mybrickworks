# Databricks notebook source
from utils import read_from_silver
from pyspark.sql.functions import col, countDistinct, sum as _sum, count as _count, when, round as _round, avg

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

order_items_valid_sellers =\
(
    order_items
    .select("order_id","seller_id")
    .join(valid_sellers, on="seller_id", how="inner")
    .distinct()
)

# COMMAND ----------

seller_time =\
orders.select("order_id","is_late").join(
    order_items_valid_sellers,
    on="order_id",
    how="inner"
).groupby(
    "seller_id"
).agg(
    _round(_sum(when(~col("is_late"), 1).otherwise(0)) / countDistinct(col("order_id")), 2).alias("on_time_rate")
).withColumn("late_order_pct", round(1 - col("on_time_rate"), 2))

# COMMAND ----------

seller_earnings =\
order_items_valid_sellers.groupby("seller_id","seller_state").agg(
    countDistinct(col("order_id")).alias("total_orders"),
    _sum(col("price")).alias("total_revenue")
)

# COMMAND ----------

seller_earnings.display()

# COMMAND ----------

seller_time.display()

# COMMAND ----------

orders.display()

# COMMAND ----------

order_items.display()

# COMMAND ----------

seller_reviews =\
order_items_valid_sellers.join(
    reviews,
    on="order_id",
    how="inner"
).groupby("seller_id").agg(
    _round(avg(col("review_score")), 2).alias("avg_review_score")
)
