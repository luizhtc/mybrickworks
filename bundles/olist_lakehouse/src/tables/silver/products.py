# Databricks notebook source
from pyspark.sql.functions import coalesce, col, lit
from bundles.olist_lakehouse.src.tables.silver.utils import read_from_bronze

# COMMAND ----------

products = read_from_bronze("tbl_olist_products_dataset")
translations = read_from_bronze("tbl_product_category_name_translation")

# COMMAND ----------

products_join = products.join(translations, on="product_category_name", how="left")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Calculated columns

# COMMAND ----------

products_calc = products_join.withColumn(
    "product_volume_cm3",
    col("product_length_cm").cast("int")
    * col("product_height_cm").cast("int")
    * col("product_width_cm").cast("int"),
).withColumn(
    "density_g_cm3", col("product_weight_g").cast("int") / col("product_volume_cm3")
)

# COMMAND ----------

products_transformed = products_calc.select(
    "product_id",
    coalesce(col("product_category_name"), lit("desconhecido")).alias(
        "product_category_name"
    ),
    coalesce(col("product_category_name_english"), lit("unknown")).alias(
        "product_category_name_english"
    ),
    col("product_name_lenght").cast("int").alias("product_name_length"),
    col("product_description_lenght").cast("int").alias("product_description_length"),
    col("product_photos_qty").cast("int"),
    col("product_weight_g").cast("int"),
    "product_volume_cm3",
    "density_g_cm3",
)

# COMMAND ----------

(
    products_transformed.write.format("delta")
    .mode("overwrite")
    .saveAsTable("`cat_olist`.`sch_silver`.`products`")
)
