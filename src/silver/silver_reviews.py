# Databricks notebook source
from pyspark.sql.functions import col, when, trim, timestamp_diff
from utils import read_from_bronze, parse_timestamp

# COMMAND ----------

reviews = read_from_bronze("tbl_olist_order_reviews_dataset")

# COMMAND ----------

reviews_select =\
reviews.select(
    "review_id",
    "order_id",
    col("review_score").cast("int"),
    "review_comment_title",
    "review_comment_message",
    parse_timestamp(col("review_creation_date")).alias("review_creation_date"),
    parse_timestamp(col("review_answer_timestamp")).alias("review_answer_timestamp")
)

# COMMAND ----------

silver_reviews = (
    reviews_select
        .withColumn(
            "sentiment_bucket",
            when(col("review_score").isin([1,2]), "negative")
            .when(col("review_score") == 3, "neutral")
            .when(col("review_score").isin([4,5]), "positive")
            .otherwise(None)
        )
        .withColumn(
            "has_comment",
            when(trim(col("review_comment_message")).isNull(), False)
            .otherwise(True)
        )
        .withColumn(
            "response_time_hours",
            timestamp_diff("HOUR", col("review_creation_date"), col("review_answer_timestamp"))
        )
)

# COMMAND ----------

silver_reviews.write.format("delta").mode("overwrite").saveAsTable(f"`cat_olist`.`sch_silver`.`silver_reviews`")
