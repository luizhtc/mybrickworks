from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql.functions import coalesce, lit, try_to_timestamp


def read_from_bronze(table: str, spark: SparkSession | None = None) -> DataFrame:
    """Reads a table from the Catalog 'cat_olist' and Schema 'sch_bronze'

    Args:
        table (string): The table name to be read.

    Returns:
        DataFrame: The PySpark DataFrame loaded with the table contents.
    """
    spark = spark or SparkSession.getActiveSession()
    if spark is None:
        raise RuntimeError("No active SparkSession found.")

    CATALOG = "cat_olist"
    SCHEMA = "sch_bronze"

    df = spark.read.table(f"{CATALOG}.{SCHEMA}.{table}")
    return df


def parse_timestamp(column: Column):
    """Parses a string formatted column into a timestamp column given formats

    Args:
        column (Column): The column to be parsed.

    Returns:
        Column: The parsed column with the timestamp information or NULL in case of
        parsing failure.
    """
    FORMATS = ["yyyy-MM-dd HH:mm:ss"]
    return coalesce(*[try_to_timestamp(column, lit(fmt)) for fmt in FORMATS])
