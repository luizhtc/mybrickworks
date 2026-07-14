from pyspark.sql import Column, DataFrame
from pyspark.sql.functions import current_timestamp, coalesce, lit, try_to_timestamp


def add_metadata(df: DataFrame) -> DataFrame:
    """Adds metadata columns to the PySpark DataFrame

    Args:
        df (DataFrame): The PySpark DataFrame to add metadata to.

    Returns:
        DataFrame: The PySpark DataFrame with the metadata columns added.
    """
    df = df.withColumn("meta_updated_ts", current_timestamp())
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
