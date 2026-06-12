# Databricks notebook source
from pyspark.sql.functions import col, timestamp_diff, trim, when
from bundles.olist_lakehouse.src.tables.silver.utils import parse_timestamp, read_from_bronze

# COMMAND ----------

reviews = read_from_bronze("tbl_olist_order_reviews_dataset")

# COMMAND ----------

reviews_select = reviews.select(
    "review_id",
    "order_id",
    col("review_score").cast("int"),
    "review_comment_title",
    "review_comment_message",
    parse_timestamp(col("review_creation_date")).alias("review_creation_date"),
    parse_timestamp(col("review_answer_timestamp")).alias("review_answer_timestamp"),
)

# COMMAND ----------

reviews_transformed = (
    reviews_select.withColumn(
        "sentiment_bucket",
        when(col("review_score").isin([1, 2]), "negative")
        .when(col("review_score") == 3, "neutral")
        .when(col("review_score").isin([4, 5]), "positive")
        .otherwise(None),
    )
    .withColumn(
        "has_comment",
        when(trim(col("review_comment_message")).isNull(), False).otherwise(True),
    )
    .withColumn(
        "response_time_hours",
        timestamp_diff(
            "HOUR", col("review_creation_date"), col("review_answer_timestamp")
        ),
    )
)

# COMMAND ----------

(
    reviews_transformed.write.format("delta")
    .mode("overwrite")
    .saveAsTable("`cat_olist`.`sch_silver`.`reviews`")
)
