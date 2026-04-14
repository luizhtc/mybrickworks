# Databricks notebook source
from pyspark.sql.functions import col
from utils import read_from_bronze

# COMMAND ----------

products = read_from_bronze("tbl_olist_products_dataset")
translations = read_from_bronze("tbl_product_category_name_translation")

# COMMAND ----------

products_join =\
products.join(
    translations,
    on="product_category_name",
    how="left"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Calculated columns

# COMMAND ----------

products_calc = (
    products_join
        .withColumn(
            "product_volume_cm3",
            col("product_length_cm").cast("int")
            * col("product_height_cm").cast("int")
            * col("product_width_cm").cast("int")
        )
        .withColumn(
            "density_g_cm3",
            col("product_weight_g").cast("int") / col("product_volume_cm3")
        )
)

# COMMAND ----------

silver_products =\
products_calc.select(
    "product_id",
    "product_category_name",
    "product_category_name_english",
    col("product_name_lenght").cast("int"),
    col("product_description_lenght").cast("int"),
    col("product_photos_qty").cast("int"),
    col("product_weight_g").cast("int"),
    "product_volume_cm3",
    "density_g_cm3"
)

# COMMAND ----------

silver_products.write.format("delta").mode("overwrite").saveAsTable(f"`cat_olist`.`sch_silver`.`silver_products`")
