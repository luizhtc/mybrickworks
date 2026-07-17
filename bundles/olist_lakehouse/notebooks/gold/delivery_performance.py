# Databricks notebook source
from pyspark.sql import Window
from pyspark.sql.functions import (
    avg,
    col,
    countDistinct,
    rank,
    when,
    max as _max,
    round as _round,
    sum as _sum,
)

# COMMAND ----------

dbutils.widgets.text("in_catalog", "")
dbutils.widgets.text("in_schema", "")
dbutils.widgets.text("out_catalog", "")
dbutils.widgets.text("out_schema", "")

# COMMAND ----------

in_catalog = dbutils.widgets.get("in_catalog")
in_schema = dbutils.widgets.get("in_schema")
out_catalog = dbutils.widgets.get("out_catalog")
out_schema = dbutils.widgets.get("out_schema")

# COMMAND ----------

orders = spark.read.table(f"{in_catalog}.{in_schema}.orders")
order_items = spark.read.table(f"{in_catalog}.{in_schema}.order_items")

# COMMAND ----------

pre_group_orders = (
    order_items.join(orders, on="order_id", how="inner")
    .select("order_id", "seller_state", "delivery_delay_days", "is_late")
    .distinct()
)

# COMMAND ----------

delivery_performance = (
    pre_group_orders.groupby("seller_state")
    .agg(
        countDistinct(col("order_id")).alias("total_orders"),
        _sum(when(col("is_late") is False, 1).otherwise(0)).alias("on_time_orders"),
        _sum(when(col("is_late") is True, 1).otherwise(0)).alias("late_orders"),
        avg(col("delivery_delay_days")).alias("avg_delay_days"),
        _max(col("delivery_delay_days")).alias("max_delay_days"),
    )
    .withColumn("on_time_rate", _round(col("on_time_orders") / col("total_orders"), 2))
)

# COMMAND ----------

wn_performance = Window.orderBy(col("on_time_rate").desc())
delivery_performance_ranked = delivery_performance.withColumn("performance_rank", rank().over(wn_performance))

# COMMAND ----------

(
    delivery_performance_ranked.write.format("delta")
    .mode("overwrite")
    .saveAsTable("`cat_olist`.`sch_gold`.`delivery_performance`")
)
