# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step - 1 Read lap_times file using dataframe API

# COMMAND ----------

from pyspark.sql.types import StringType,IntegerType, StructField, StructType,FloatType

# COMMAND ----------

lap_times_schema = StructType ( fields = [ 
  
  StructField ("raceId",IntegerType(),False),
  StructField("driverId",IntegerType(),True),
  StructField("lap",IntegerType(),True),
  StructField("position",IntegerType(),True),
  StructField("time",StringType(),True),
  StructField("milliseconds",IntegerType(),True)    
]
)

# COMMAND ----------

lap_times_df = spark.read.schema(lap_times_schema)\
.csv(f"{raw_folder_path}/lap_times/")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step - 2 Rename and add the column

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

final_renamed_df = lap_times_df.withColumnRenamed("driverId","driver_id")\
.withColumnRenamed("raceId","race_id")\
.withColumn("Ingestion_date",current_timestamp())

# COMMAND ----------

final_df = add_ingestion_date(final_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step - 3  Write the parquet file to processed container

# COMMAND ----------

final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/lap_times")

# COMMAND ----------

dbutils.notebook.exit("Success")
