from pyspark.sql import DataFrame
from pyspark.sql.functions import col


def add_metadata(df: DataFrame) -> DataFrame:
    """Adds metadata columns to the PySpark DataFrame

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
        .withColumn("meta_file_modification_time", col("_metadata.file_modification_time"))
    )
    return df
