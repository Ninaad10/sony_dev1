# Databricks notebook source
from pyspark.sql import Row
import datetime
from pyspark.sql.functions import *

# COMMAND ----------

users=[
    {
        "id":1,
        "name":"Sachin",
        "last_name":"Tendulkar",
        "email":"sachin@gmail.com",
        "Mobile":Row(mobile= "342779900", home= "91568557700"),
        "courses": [1,2],
        "is_customer":True,
        "DOB": datetime.date(1973,4,24)
    },
    {
         "id":2,
        "name":"Virat",
        "last_name":"Kohli",
        "email":"virat@gmail.com",
        "Mobile":Row(mobile= "91556600", home= "918912300"),
        "courses": [2,3],
        "is_customer":True, 
        "DOB":datetime.date(1988,11,5)
    },
     {
         "id":3,
        "name":"Rohit",
        "last_name":"Sharma",
        "email":"rohit@gmail.com",
        "Mobile":Row(mobile= "914455700", home= "9145997700"),
        "courses": [3],
        "is_customer":True, 
        "DOB":datetime.date(1987,4,30)
     },
     {
         "id":4,
        "name":"Dinesh",
        "last_name":"Karthik",
        "email":"dinesh@gmail.com",
        "Mobile":Row(mobile= "91467700", home= "916789700"),
        "courses": [3],
        "is_customer":True, 
        "DOB":datetime.date(1985,6,1)
     },
     {
         "id":5,
        "name":"M S",
        "last_name":"Dhoni",
        "email":"dhoni@gmail.com",
        "Mobile":Row(mobile= "91467799", home= "916778800"),
        "courses": [3],
        "is_customer":True, 
        "DOB":datetime.date(1981,7,7)
     }
]
 

# COMMAND ----------

df = spark.createDataFrame(data=users)
df.display()

# COMMAND ----------

df.withColumnRenamed("id","user_id").display()

# COMMAND ----------

df_final = df.withColumn("current_timestamp",current_timestamp())\
    .withColumnRenamed("id","user_id")\
    .withColumn("full_name", concat(col("name"),lit(" "),col("last_name")))\
    .drop("is_customer")\
    .select("user_id","full_name","email","DOB","mobile","courses","current_timestamp")
df_final.display()


# COMMAND ----------

df_final = df_final.withColumn("mobile_office",col("mobile.mobile"))\
    .withColumn("mobile_home",col("mobile.home"))\
        .drop("mobile")\
        .withColumn("courses",explode("courses"))
df_final.display()

# COMMAND ----------


