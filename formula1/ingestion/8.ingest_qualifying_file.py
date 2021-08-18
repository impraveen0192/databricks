# Databricks notebook source
# MAGIC %md
# MAGIC ##### Step - 1 Read qualifying json file using dataframe API

# COMMAND ----------

from pyspark.sql.types import StringType,IntegerType, StructField, StructType,FloatType

# COMMAND ----------

qualifying_schema = StructType ( fields = [ 
  
  StructField ("qualifyId",IntegerType(),False),
  StructField("raceId",IntegerType(),False),
  StructField("driverId",IntegerType(),False),
  StructField("constructorId",IntegerType(),False),
  StructField("number",IntegerType(),False),
  StructField("position",IntegerType(),True),
  StructField("q1",StringType(),True),
  StructField("q2",StringType(),True),
  StructField("q3",StringType(),True)
]
)

# COMMAND ----------

qualifying_df = spark.read.schema(qualifying_schema)\
.option("multiLine",True)\
.json("/mnt/sapkformula1dl/raw/qualifying/")

# COMMAND ----------

qualifying_df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step - 2 Rename and add the column

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

final_df = qualifying_df.withColumnRenamed("qualifyingId","qualifying_id")\
.withColumnRenamed("raceId","race_id")\
.withColumnRenamed("driverId","driver_id")\
.withColumnRenamed("constructorId","constructor_id")\
.withColumn("Ingestion_date",current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step - 3  Write the parquet file to processed container

# COMMAND ----------

final_df.write.mode("overwrite").parquet("/mnt/sapkformula1dl/processed/qualifying")

# COMMAND ----------


