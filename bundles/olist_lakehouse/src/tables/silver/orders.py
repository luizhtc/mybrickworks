# Databricks notebook source
from pyspark.sql.functions import (
    col,
    collect_set,
    datediff,
    when,
)
from pyspark.sql.functions import (
    max as _max,
)
from pyspark.sql.functions import (
    sum as _sum,
)
from bundles.olist_lakehouse.src.tables.silver.utils import parse_timestamp, read_from_bronze

# COMMAND ----------

orders = read_from_bronze("tbl_olist_orders_dataset")
customers = read_from_bronze("tbl_olist_customers_dataset")
payments = read_from_bronze("tbl_olist_order_payments_dataset")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Payments aggregation and data type treatment

# COMMAND ----------

payments_treated_types = payments.withColumn(
    "payment_value", col("payment_value").cast("double")
).withColumn("payment_installments", col("payment_installments").cast("int"))

# COMMAND ----------

payments_agg = (
    payments_treated_types.groupBy("order_id")
    .agg(
        _sum(col("payment_value")).alias("total_payment_value"),
        _max(col("payment_installments")).alias("max_installments"),
        collect_set(col("payment_type")).alias("payment_types_used"),
    )
    .withColumn("payment_types_used", col("payment_types_used").cast("string"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## JOIN and transform silver_orders table

# COMMAND ----------

orders_join = orders.join(customers, on="customer_id", how="left").join(
    payments_agg, on="order_id", how="left"
)

# COMMAND ----------

orders_transformed = orders_join.select(
    "order_id",
    "customer_unique_id",
    "customer_state",
    "order_status",
    parse_timestamp("order_purchase_timestamp").alias("order_purchase_timestamp"),
    "total_payment_value",
    "max_installments",
    "payment_types_used",
    when(col("order_delivered_customer_date").isNull(), None)
    .otherwise(
        datediff(
            parse_timestamp("order_delivered_customer_date"),
            parse_timestamp("order_estimated_delivery_date"),
        )
    )
    .alias("delivery_delay_days"),
).withColumn("is_late", when(col("delivery_delay_days") > 0, True).otherwise(False))

# COMMAND ----------

(
    orders_transformed.write.format("delta")
    .mode("overwrite")
    .saveAsTable("`cat_olist`.`sch_silver`.`orders`")
)
