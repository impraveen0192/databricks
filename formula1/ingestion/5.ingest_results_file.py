# Databricks notebook source
# MAGIC %md
# MAGIC #####Step 1 Read the results file

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,StringType,DoubleType,FloatType

# COMMAND ----------



# COMMAND ----------

results_schema = StructType ( fields = [
  StructField ("resultId", IntegerType(),False),
  StructField ("raceId", IntegerType(),True),
  StructField ("driverId", IntegerType(),True),
  StructField ("constructorId", IntegerType(),True),
  StructField ("number", IntegerType(),True),
  StructField ("grid", IntegerType(),True),
  StructField ("position", IntegerType(),True),
  StructField ("positionText", StringType(),True),
  StructField ("positionOrder", IntegerType(),True),
  StructField ("points", FloatType(),True),
  StructField ("laps", IntegerType(),True),
  StructField ("time", StringType(),True),
  StructField ("milliseconds", IntegerType(),True),
  StructField ("fastestLap", IntegerType(),True),
  StructField ("rank", IntegerType(),True),
  StructField ("fastestLapTime", StringType(),True),
  StructField ("fastestLapSpeed", FloatType(),True),
  StructField ("statusId", IntegerType(),True)
  
  
  
]
)

# COMMAND ----------

results_df = spark.read.schema(results_schema).json(f"{raw_folder_path}/results.json")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 Rename columns , add ingestion_date column and drop statusId column

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

results_renamed_df = results_df.withColumnRenamed("resultId", "result_id")\
.withColumnRenamed("raceId","race_id")\
.withColumnRenamed("driverId","driver_id")\
.withColumnRenamed("constructorId","constructor_id")\
.withColumnRenamed("positionText","position_text")\
.withColumnRenamed("positionOrder","position_order")\
.withColumnRenamed("fastestLap","fastest_lap")\
.withColumnRenamed("fastestLapTime","fastest_lap_time")\
.withColumnRenamed("fastestLapSpeed","fastest_lap_speed")\
.drop("statusId")

# COMMAND ----------

display(results_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 4 Add Ingestion Date

# COMMAND ----------

results_ingestion_date_df = add_ingestion_date(results_renamed_df)

# COMMAND ----------

display(results_ingestion_date_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 4 write the dataframe to processed folder

# COMMAND ----------

results_ingestion_date_df.write.mode("overwrite").partitionBy("race_id").parquet(f"{processed_folder_path}/results")

# COMMAND ----------

results_ingestion_date_df.count()

# COMMAND ----------

spark.read.parquet(f"{processed_folder_path}/results").count()

# COMMAND ----------

dbutils.notebook.exit("Success")
