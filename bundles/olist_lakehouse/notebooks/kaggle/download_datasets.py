# Databricks notebook source
import os

import kagglehub

# COMMAND ----------

# olistbr/brazilian-ecommerce is a static dataset from Kaggle,
# in this case I use a simple download so later I am able to extract
# and load the files into Unity Catalog

# This script needs a KAGGLE_API_TOKEN to run, it should already be created
# If not, you can create it via Databricks CLI:
# databricks secrets create-scope kaggle
# databricks secrets put-secret kaggle KAGGLE_API_TOKEN --string-value "<your_token>"

# COMMAND ----------

# Get Kaggle API Token
os.environ["KAGGLE_API_TOKEN"] = dbutils.secrets.get("kaggle", "KAGGLE_API_TOKEN")

# Download to driver
datasets_path = kagglehub.dataset_download(
    "olistbr/brazilian-ecommerce",
    output_dir="/Volumes/cat_olist/sch_bronze/vol_landing/olist_kaggle_datasets/"
)

# COMMAND ----------

print(f"Downloaded successfully to: \n{datasets_path}")
