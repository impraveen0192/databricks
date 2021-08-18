# Databricks notebook source
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
.csv("/mnt/sapkformula1dl/raw/lap_times/")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step - 2 Rename and add the column

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

final_df = lap_times_df.withColumnRenamed("driverId","driver_id")\
.withColumnRenamed("raceId","race_id")\
.withColumn("Ingestion_date",current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step - 3  Write the parquet file to processed container

# COMMAND ----------

final_df.write.mode("overwrite").parquet("/mnt/sapkformula1dl/processed/lap_times")

# COMMAND ----------


