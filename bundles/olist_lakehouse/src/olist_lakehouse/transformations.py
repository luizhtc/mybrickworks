from pyspark.sql import DataFrame
from pyspark.sql.functions import current_timestamp


def add_metadata(df: DataFrame) -> DataFrame:
    """Adds metadata columns to the PySpark DataFrame

    Args:
        df (DataFrame): The PySpark DataFrame to add metadata to.

    Returns:
        DataFrame: The PySpark DataFrame with the metadata columns added.
    """
    df = df.withColumn("meta_updated_ts", current_timestamp())
    return df
