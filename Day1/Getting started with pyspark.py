# Databricks notebook source
print("Hello")

# COMMAND ----------

data = [(1,'a',20),(2,'b',30)]
schema = ["id","name","age"]
df = spark.createDataFrame(data,schema)
df.display()


# COMMAND ----------

df.select("*").display()

# COMMAND ----------

df1 = df.select("id","name")

# COMMAND ----------

df1.display()

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

df.select(col("id").alias("emp_id")).display()

# COMMAND ----------

df.withColumnRenamed("id","emp_id").display()

# COMMAND ----------

df.withColumnsRenamed({"id":"emp_id","name":"emp_name","age":"emp_age"}).display()

# COMMAND ----------

df.withColumn("current_date",current_timestamp()).display()

# COMMAND ----------

df.withColumn("age",lit(23)).display()

# COMMAND ----------


