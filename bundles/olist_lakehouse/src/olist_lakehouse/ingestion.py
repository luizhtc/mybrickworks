"""
olistbr/brazilian-ecommerce is a static dataset from Kaggle,
in this case I use a simple download so later I am able to extract
and load the files into Unity Catalog

This requires a Kaggle API Token, defined in 'kaggle' databricks secrets scope:
databricks secrets create-scope kaggle
databricks secrets put-secret kaggle KAGGLE_API_TOKEN --string-value "<your_token>"
"""

import os

import kagglehub
from databricks.sdk.errors.platform import ResourceDoesNotExist

def download_datasets():
    try:
        # Get Kaggle API Token
        os.environ["KAGGLE_API_TOKEN"] = dbutils.secrets.get("kaggle", "KAGGLE_API_TOKEN")
    except ResourceDoesNotExist as e:
        raise Exception(
            "Kaggle API Token not found in Databricks secrets. "
            "Please create a secret scope named 'kaggle' and add your Kaggle API token as 'KAGGLE_API_TOKEN'."
        ) from e

    # Download to volume
    datasets_path = kagglehub.dataset_download(
        "olistbr/brazilian-ecommerce",
        output_dir="/Volumes/cat_olist/sch_bronze/vol_landing/kaggle_datasets/",
    )

    print(f"Successfully downloaded to: \n{datasets_path}")
