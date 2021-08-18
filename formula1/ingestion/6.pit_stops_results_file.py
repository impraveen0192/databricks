# Databricks notebook source
# MAGIC %md
# MAGIC ##### Step - 1 Read Multiline json file

# COMMAND ----------

from pyspark.sql.types import StringType,IntegerType, StructField, StructType,FloatType

# COMMAND ----------

pitstops_schema = StructType ( fields = [ 
  
  StructField ("raceId",IntegerType(),False),
  StructField("driverId",IntegerType(),True),
  StructField("stop",StringType(),True),
  StructField("lap",IntegerType(),True),
  StructField("time",StringType(),True),
  StructField("duration",StringType(),True),
  StructField("milliseconds",IntegerType(),True)    
]
)

# COMMAND ----------

pitstops_df = spark.read.schema(pitstops_schema)\
.option("multiLine",True)\
.json("/mnt/sapkformula1dl/raw/pit_stops.json")

# COMMAND ----------

display(pitstops_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step - 2 Rename and add the column

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

final_df = pitstops_df.withColumnRenamed("driverId","driver_id")\
.withColumnRenamed("raceId","race_id")\
.withColumn("Ingestion_date",current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step - 3  Write the parquet file to processed container

# COMMAND ----------

final_df.write.mode("overwrite").parquet("/mnt/sapkformula1dl/processed/pt_stops")

# COMMAND ----------


