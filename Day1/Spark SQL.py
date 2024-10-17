# Databricks notebook source
# MAGIC %sql
# MAGIC create table products as
# MAGIC select *,current_timestamp() as ingested_timestamp from json.`/Volumes/nin_databricks/default/raw/products.json`

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table customer

# COMMAND ----------


