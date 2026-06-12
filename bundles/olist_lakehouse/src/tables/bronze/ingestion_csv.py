# Databricks notebook source
import os

from pyspark.sql import DataFrame
from pyspark.sql.functions import col

# COMMAND ----------


def add_metadata(df: DataFrame) -> DataFrame:
    """Adds metadata column to the PySpark DataFrame

    Args:
        df (DataFrame): The PySpark DataFrame to add metadata to.

    Returns:
        DataFrame: The PySpark DataFrame with the metadata columns added.
    """
    df = (
        df.withColumn("meta_file_path", col("_metadata.file_path"))
        .withColumn("meta_file_name", col("_metadata.file_name"))
        .withColumn("meta_file_size", col("_metadata.file_size"))
        .withColumn("meta_file_block_start", col("_metadata.file_block_start"))
        .withColumn("meta_file_block_length", col("_metadata.file_block_length"))
        .withColumn(
            "meta_file_modification_time", col("_metadata.file_modification_time")
        )
    )
    return df


# COMMAND ----------

dbutils.widgets.text("csv_file_name", "")
csv_file_name = dbutils.widgets.get("csv_file_name")
if not csv_file_name.endswith(".csv"):
    csv_file_name = csv_file_name + ".csv"

# COMMAND ----------

csv_file_path = os.path.join(
    "/Volumes/cat_olist/sch_bronze/vol_landing/olist/", csv_file_name
)
df = (
    spark.read.option("header", True)
    .option("multiLine", True)
    .option("escape", '"')
    .format("csv")
    .load(csv_file_path)
    .transform(add_metadata)
)

# COMMAND ----------

table_name = f"tbl_{csv_file_name.replace('.csv', '')}"
df.write.format("delta").mode("overwrite").saveAsTable(
    f"`cat_olist`.`sch_bronze`.`{table_name}`"
)
