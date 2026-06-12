# Databricks notebook source
from pyspark.sql import Window
from pyspark.sql.functions import (
    avg,
    col,
    countDistinct,
    rank,
)
from pyspark.sql.functions import (
    count as _count,
)
from pyspark.sql.functions import (
    sum as _sum,
)
from bundles.olist_lakehouse.src.tables.gold.utils import read_from_silver

# COMMAND ----------

orders = read_from_silver("orders")
order_items = read_from_silver("order_items")
products = read_from_silver("products")

# COMMAND ----------

orders_and_products = order_items.join(products, on="product_id", how="left").join(
    orders, on="order_id", how="left"
)

# COMMAND ----------

orders_and_products_filtered = orders_and_products.filter("order_status = 'delivered'")

# COMMAND ----------

sales_by_category = orders_and_products_filtered.groupBy(
    "product_category_name_english"
).agg(
    _sum("price").alias("total_revenue"),
    countDistinct("order_id").alias("total_orders"),
    _count("*").alias("total_items_sold"),
    (_sum("price") / countDistinct("order_id")).alias("avg_order_ticket"),
    avg("price").alias("avg_item_price"),
)

# COMMAND ----------

wn_revenue = Window.orderBy(col("total_revenue").desc(), col("avg_order_ticket").desc())

sales_by_category_ranked = sales_by_category.withColumn(
    "revenue_rank", rank().over(wn_revenue)
)

# COMMAND ----------

(
    sales_by_category_ranked.write.format("delta")
    .mode("overwrite")
    .saveAsTable("`cat_olist`.`sch_gold`.`sales_by_category`")
)
