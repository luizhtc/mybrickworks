# Databricks notebook source
import os

import kagglehub

# COMMAND ----------

# Get Kaggle API Token
os.environ["KAGGLE_API_TOKEN"] = dbutils.secrets.get("kaggle", "KAGGLE_API_TOKEN")
# Download to driver
datasets_path = kagglehub.dataset_download("olistbr/brazilian-ecommerce")
# Move to volume
dbutils.fs.cp(datasets_path, "/Volumes/cat_olist/sch_bronze/vol_landing/olist_kaggle_datasets/", recurse=True)
