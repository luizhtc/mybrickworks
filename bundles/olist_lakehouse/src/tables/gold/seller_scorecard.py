# Databricks notebook source
from pyspark.sql.functions import (
    avg,
    col,
    countDistinct,
    lit,
    when,
)
from pyspark.sql.functions import (
    round as _round,
)
from pyspark.sql.functions import (
    sum as _sum,
)
from bundles.olist_lakehouse.src.tables.gold.utils import read_from_silver

# COMMAND ----------

orders = read_from_silver("orders")
order_items = read_from_silver("order_items")
reviews = read_from_silver("reviews")

# COMMAND ----------

valid_sellers = (
    orders.filter(col("order_status") == "delivered")
    .join(order_items, on="order_id", how="left")
    .groupBy("seller_id", "seller_state")
    .agg(countDistinct(col("order_id")).alias("total_delivered_orders"))
    .filter(col("total_delivered_orders") >= 20)
    .select("seller_id", "seller_state")
)

# COMMAND ----------

order_items_valid_sellers = (
    order_items.join(valid_sellers, on="seller_id", how="inner")
    .select("order_id", "seller_id")
    .distinct()
)

# COMMAND ----------

seller_time = (
    orders.join(order_items_valid_sellers, on="order_id", how="inner")
    .groupBy("seller_id")
    .agg(
        _round(
            _sum(when(~col("is_late"), 1).otherwise(0))
            / countDistinct(col("order_id")),
            2,
        ).alias("on_time_rate")
    )
    .withColumn("late_order_pct", _round(1 - col("on_time_rate"), 2))
)

# COMMAND ----------

seller_earnings = (
    order_items_valid_sellers.join(
        order_items, on=["order_id", "seller_id"], how="inner"
    )
    .groupBy(col("seller_id"))
    .agg(
        countDistinct(col("order_id")).alias("total_orders"),
        _round(_sum(col("price")), 2).alias("total_revenue"),
    )
)

# COMMAND ----------

seller_reviews = (
    order_items_valid_sellers.join(reviews, on="order_id", how="inner")
    .groupBy("seller_id")
    .agg(_round(avg(col("review_score")), 2).alias("avg_review_score"))
)

# COMMAND ----------

seller_scorecard = (
    valid_sellers.join(seller_earnings, on="seller_id", how="inner")
    .join(seller_reviews, on="seller_id", how="inner")
    .join(seller_time, on="seller_id", how="inner")
    .withColumn(
        "seller_tier",
        when((col("avg_review_score") >= 4.5) & (col("on_time_rate") >= 0.9), "top")
        .when((col("avg_review_score") < 3.5) | (col("on_time_rate") < 0.7), "at risk")
        .otherwise(lit("regular")),
    )
)

# COMMAND ----------

(
    seller_scorecard.write.format("delta")
    .mode("overwrite")
    .saveAsTable("`cat_olist`.`sch_gold`.`seller_scorecard`")
)
