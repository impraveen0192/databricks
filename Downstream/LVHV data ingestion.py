# Databricks notebook source
# MAGIC %run "../Downstream/includes/configuration"

# COMMAND ----------

# MAGIC %md 
# MAGIC My name is praveen

# COMMAND ----------

df = spark.read.csv(f"{raw_folder_path}/circuits*")
Test = 10

# COMMAND ----------


