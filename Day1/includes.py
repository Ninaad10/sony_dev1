# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

input_path = "/Volumes/nin_databricks/default/raw/"

# COMMAND ----------

def add_ingestion(df):
    df1 = df.withColumn("ingested_timestamp",current_timestamp())
    return df1 
