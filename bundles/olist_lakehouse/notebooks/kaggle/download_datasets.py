# Databricks notebook source
import os

import kagglehub

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
