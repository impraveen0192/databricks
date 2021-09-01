# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

eu_mart_geo_df = spark.read.parquet(f"{silver_folder_path}/EU_MART_GEO_parquet")


# COMMAND ----------

display(eu_mart_geo_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ####1 Write data to file location in delta format

# COMMAND ----------

  eu_mart_geo_df.write.format("delta").mode("overwrite").save(f"{gold_folder_path}/eu_mart_geo_external")

# COMMAND ----------

  eu_mart_geo_df.write.parquet(f"{gold_folder_path}/test_parquet")

# COMMAND ----------

read_eu_mart_geo = spark.read.format("delta").load("/mnt/sapkformula1dl/gold/eu_mart_geo_external")

# COMMAND ----------

# MAGIC %sql 
# MAGIC create database if not exists demo;

# COMMAND ----------

# MAGIC %md 
# MAGIC ####2. creating Managed table with parquet

# COMMAND ----------

eu_mart_geo_df.write.mode("overwrite").format("parquet").saveAsTable("demo.eu_mart_geo_parquet")


# COMMAND ----------

# MAGIC %md 
# MAGIC ####3. creating Managed table with Delta

# COMMAND ----------

eu_mart_geo_df.write.mode("overwrite").format("delta").saveAsTable("demo.eu_mart_geo_delta")

# COMMAND ----------

# MAGIC %sql
# MAGIC use demo ;
# MAGIC show tables;

# COMMAND ----------

# MAGIC %sql
# MAGIC desc extended eu_mart_geo_parquet

# COMMAND ----------

# MAGIC %sql
# MAGIC desc extended eu_mart_geo_delta

# COMMAND ----------

eu_mart_geo_df.write.mode("overwrite").saveAsTable("demo.eu_mart_geo_default")

# COMMAND ----------

# MAGIC %sql
# MAGIC desc extended eu_mart_geo_default

# COMMAND ----------

# MAGIC %md
# MAGIC #####4 Create external table

# COMMAND ----------

# MAGIC %sql
# MAGIC Create Table if not exists demo.eu_mart_geo_external_table
# MAGIC USING DELTA
# MAGIC location '/mnt/sapkformula1dl/gold/eu_mart_geo_external'

# COMMAND ----------

# MAGIC %sql
# MAGIC desc extended eu_mart_geo_external_table

# COMMAND ----------

# MAGIC %md
# MAGIC ####4. Updates 

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from eu_mart_geo_delta

# COMMAND ----------

# MAGIC %sql
# MAGIC Update eu_mart_geo_delta
# MAGIC Set region = "South"
# MAGIC Where Order_id = 'BN-2011-7407039'

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from eu_mart_geo_delta

# COMMAND ----------

from delta.tables import DeltaTable



# COMMAND ----------

update_delta_python = DeltaTable.forPath(spark,f"{gold_folder_path}/eu_mart_geo_external")

# COMMAND ----------

update_delta_python.update("Order_id = 'BN-2011-7407039'",{"Region":"'TestRegion'"})

# COMMAND ----------

display(spark.read.format("delta").load(f"{gold_folder_path}/eu_mart_geo_external").filter("Order_ID = 'BN-2011-7407039'"))

# COMMAND ----------

# MAGIC %md
# MAGIC ####5. Delete 

# COMMAND ----------

update_delta_python.delete("Order_id = 'BN-2011-7407039'")

# COMMAND ----------

display(spark.read.format("delta").load(f"{gold_folder_path}/eu_mart_geo_external").filter("Order_ID = 'BN-2011-7407039'"))

# COMMAND ----------

# MAGIC %md
# MAGIC ####6 History & versioning

# COMMAND ----------

# MAGIC %md
# MAGIC #####6.1 Using Sql

# COMMAND ----------

# MAGIC %sql 
# MAGIC Desc History demo.eu_mart_geo_delta

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from eu_mart_geo_delta version as of 4

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from eu_mart_geo_delta timestamp as of '2021-08-26T10:35:25.000+0000'

# COMMAND ----------

# MAGIC %md
# MAGIC #####6.2 Using python

# COMMAND ----------

# MAGIC %sql
# MAGIC Desc History eu_mart_geo_external_table

# COMMAND ----------

df = spark.read.format("delta").option("timestampAsof",'2021-08-26T10:44:14.000+0000').load("/mnt/sapkformula1dl/gold/eu_mart_geo_external")


# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ####7. GDPR requirement to delete

# COMMAND ----------

# MAGIC %md
# MAGIC ######Delete older than last 7 days data 

# COMMAND ----------

# MAGIC %sql
# MAGIC VACUUM demo.eu_mart_geo_external_table ---Delete older than last 7 days data 

# COMMAND ----------

# MAGIC %md
# MAGIC #####Delete All the historical Data and only keep the current version

# COMMAND ----------

# MAGIC %sql
# MAGIC Set spark.databricks.delta.retentionDurationCheck.enabled = false;
# MAGIC VACUUM demo.eu_mart_geo_external_table Retain 0 hours

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from eu_mart_geo_external_table timestamp as of '2021-08-26T09:16:03.000+0000'

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC History eu_mart_geo_external_table

# COMMAND ----------


