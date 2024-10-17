# Databricks notebook source
# MAGIC %run /Workspace/Users/ninadh.2001@gmail.com/Day1/includes

# COMMAND ----------

df_sales = spark.read.csv(f"{input_path}sales.csv",inferSchema=True,header=True)
df_sales.display()

# COMMAND ----------

df_sales = add_ingestion(df_sales)
df_sales.display()

# COMMAND ----------

df_sales.write.mode("overWrite").option("overwriteSchema","True").saveAsTable("sales")

# COMMAND ----------


