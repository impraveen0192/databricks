# Databricks notebook source
# MAGIC %md
# MAGIC ##### Ingest races file

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1- Read the CSV file using the spark dataframe reader API

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/sapkformula1dl/raw

# COMMAND ----------

from pyspark.sql.types import StructType ,StructField, IntegerType, StringType, DateType

# COMMAND ----------

races_schema = StructType(fields = [
  StructField("raceid",IntegerType(),False),
    StructField("year",IntegerType(),True),
    StructField("round",IntegerType(),True),
    StructField("circuitId",IntegerType(),True),
    StructField("name",StringType(),True),
    StructField("date",DateType(),True),
    StructField("time",StringType(),True),
  StructField("url",StringType(),True),
  
  
])

# COMMAND ----------

races_df = spark.read.option("header",True).schema(races_schema).csv(f"{raw_folder_path}/races.csv")

# COMMAND ----------

display(races_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Step 2 - Add ingestion date and race_timeframe to the dataframe

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, to_timestamp,concat,col,lit

# COMMAND ----------

races_with_ingestion_timestamp_df = add_ingestion_date(races_df)

# COMMAND ----------

races_with_timestamp_df = races_with_ingestion_timestamp_df.withColumn("race_timestamp",to_timestamp(concat(col('date'),lit(' '),col('time')), 'yyy-MM-dd HH:mm:ss'))

# COMMAND ----------

display(races_with_timestamp_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step3 - Select only the columns & rename as required

# COMMAND ----------

races_selected_df = races_with_timestamp_df.select(col('raceID').alias('race_id'),col("year").alias('race_year'),
                                                   col('round'),col("circuitId").alias('circuit_id'),col('name'),col('ingestion_date'),col('race_timestamp')
                                                  
                                                  
                                                  )

# COMMAND ----------

display(races_selected_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 4 - Write the output to processed container

# COMMAND ----------

races_selected_df.write.mode('overwrite').partitionBy("race_year").parquet(f'{processed_folder_path}/races')

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/sapkformula1dl/processed/races

# COMMAND ----------

display(spark.read.parquet(f"{processed_folder_path}/races"))

# COMMAND ----------


