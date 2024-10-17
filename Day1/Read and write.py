# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

df_customer =  spark.read.json("/Volumes/nin_databricks/default/raw/customers.json")
df_customer.display()

# COMMAND ----------

df_sales = spark.read.csv("/Volumes/nin_databricks/default/raw/sales.csv",inferSchema=True,header=True)
df_sales.display()

# COMMAND ----------

df_customer.write.saveAsTable("customer")

# COMMAND ----------

df_sales.write.saveAsTable("sales")

# COMMAND ----------


