# Databricks notebook source
# MAGIC %md
# MAGIC #### Ingest constructors.json file

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1 Read the jason file using the spark dataframe reader

# COMMAND ----------

constructors_schema = "constructorId INTEGER, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

constructor_df = spark.read \
.schema(constructors_schema) \
.json("/mnt/sapkformula1dl/raw/constructors.json")

# COMMAND ----------

display(constructor_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 Drop unwanted columns from the dataframe

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

##drop using col function
##construct_dropped_df = constructor_df.drop(col('url'))

# COMMAND ----------

### Useful when we want to  multiple dataframes and we want to specify the column of a specific dataframe. For example we have joined two dataframe and there is one comuln which is common in both the dataframes.
##constructor_dropped_df = constructor_df.drop(constructor_df['url'])

# COMMAND ----------

##Below can be useful when we want to drop multiple columns
constructor_dropped_df = constructor_df.drop('url') 

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Rename columns and add ingestion date

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

constructor_final_df = constructor_dropped_df.withColumnRenamed("constructorId","constructor_id") \
                                            .withColumnRenamed("constructorRef","constructor_ref") \
                                            .withColumn("ingestion_date",current_timestamp())

# COMMAND ----------

display(constructor_final_df)

# COMMAND ----------

constructor_final_df.write.mode("overwrite").parquet("/mnt/sapkformula1dl/processed/constructors")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/sapkformula1dl/processed/constructors

# COMMAND ----------


