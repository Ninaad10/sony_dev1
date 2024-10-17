# Databricks notebook source
df_sales = spark.table("sales")
df_sales.display()

# COMMAND ----------

df_customers = spark.table("customers")

# COMMAND ----------

df_joined = df_sales.join(df_customers,"customer_id","inner")
# df_joined = df_sales.join(df_customers,df_customers["customer_id"]==df_sales["customer_id"],"inner")

# COMMAND ----------

df_joined.display()

# COMMAND ----------

from pyspark.sql.functions import *
# df_customers.where("customer_id=2").display()
df_customers.filter(col("customer_id")==2).display()

# COMMAND ----------

df_customers.sort(col("customer_name").desc()).display()

# COMMAND ----------

df_joined.groupBy("customer_id").count().orderBy("customer_id").display()

# COMMAND ----------


