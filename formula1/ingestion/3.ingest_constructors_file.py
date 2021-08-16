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

### Useful when we want to  multiple dataframes and we want to specify the column of a specific dataframe. FOr example we have joined two dataframe and there is one comuln which is common in both the dataframes.
constructor_dropped_df = constructor_df.drop(constructor_df['url'])

# COMMAND ----------

constructor_dropped_df = constructor_df.drop('url') 

# COMMAND ----------


