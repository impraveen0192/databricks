# Databricks notebook source
# MAGIC %md
# MAGIC #####Step 1 Read all the data as required
# MAGIC 1. drivers 
# MAGIC 2. constructors
# MAGIC 3. results
# MAGIC 4. races
# MAGIC 5. circuits

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

drivers_df= spark.read.parquet(f"{processed_folder_path}/drivers")\
.withColumnRenamed("name","drivers_name")\
.withColumnRenamed("number","drivers_number")\
.withColumnRenamed("nationality","drivers_nationality")

# COMMAND ----------

constructors_df= spark.read.parquet(f"{processed_folder_path}/constructors")\
.withColumnRenamed("name","Team_Name")

# COMMAND ----------

results_df= spark.read.parquet(f"{processed_folder_path}/results")\
.withColumnRenamed("time","race_time")

# COMMAND ----------

races_df= spark.read.parquet(f"{processed_folder_path}/races")\
.withColumnRenamed("name","race_name")\
.withColumnRenamed("race_timestamp","race_date")

# COMMAND ----------

circuits_df= spark.read.parquet(f"{processed_folder_path}/circuits")\
.withColumnRenamed("location","circuit_location")

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step 2 join circuits to the races 

# COMMAND ----------

race_circuit_df = races_df.join(circuits_df,races_df.circuit_id == circuits_df.circuit_id,"inner")\
.select(races_df.race_id,races_df.race_year,races_df.race_name,races_df.race_date,circuits_df.circuit_location)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 Join Results to all other datafames

# COMMAND ----------

race_results_df = results_df.join(race_circuit_df,results_df.race_id == race_circuit_df.race_id)\
                  .join(drivers_df,results_df.driver_id==drivers_df.driver_id)\
                  .join(constructors_df,results_df.constructor_id == constructors_df.constructor_id)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

final_df = race_results_df.select("race_year","race_name","race_date","circuit_location","drivers_name","drivers_number","drivers_nationality",
                                  "Team_name","grid","fastest_lap","race_time","points"
                                 )\
.withColumn("created_date",current_timestamp())

# COMMAND ----------

display(final_df.filter("race_year == 2020 and race_name == 'Abu Dhabi Grand Prix'").orderBy(final_df.points.desc()))

# COMMAND ----------

final_df.write.mode("overwrite").parquet(f"{presentation_folder_path}/races_results")

# COMMAND ----------


