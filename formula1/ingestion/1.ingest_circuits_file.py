# Databricks notebook source
# MAGIC %md
# MAGIC #### Ingest circuits.csv file

# COMMAND ----------

dbutils.widgets.help()

# COMMAND ----------

dbutils.widgets.text("p_data_source","")
v_data_source = dbutils.widgets.get("p_data_source")
v_data_source

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 -Read the CSV file using the spark dataframe reader

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------



# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/sapkformula1dl/raw

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,StringType, DoubleType

# COMMAND ----------

circuits_schema = StructType(fields = [StructField("circuitId",IntegerType(),False),
                                      StructField("circuitRef",StringType(),False),
                                       StructField("name",StringType(),False),
                                       StructField("location",StringType(),False),
                                       StructField("country",StringType(),False),
                                       StructField("lat",DoubleType(),False),
                                       StructField("lng",DoubleType(),False),
                                       StructField("alt",IntegerType(),False),
                                       StructField("url",StringType(),False),
                                      
                                      
                                      ])

# COMMAND ----------

circuits_df = spark.read \
.option("header",True) \
.schema(circuits_schema) \
.csv(f"{raw_folder_path}/circuits.csv")

# COMMAND ----------

type(circuits_df)

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

circuits_df.printSchema()

# COMMAND ----------

circuits_df.describe().show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Select only the required column

# COMMAND ----------

circuits_selected_df = circuits_df.select("circuitId","circuitRef","name","location","country","lat","lng","alt")

# COMMAND ----------

circuits_selected_df = circuits_df.select(circuits_df.circuitId,circuits_df.circuitRef,circuits_df.name,circuits_df.location,circuits_df.country,circuits_df.lat,circuits_df.lng,circuits_df.alt)

# COMMAND ----------

circuits_selected_df = circuits_df.select(circuits_df["circuitId"],circuits_df["circuitRef"],circuits_df["name"],
                                          circuits_df["location"],circuits_df["country"],circuits_df["lat"],circuits_df["lng"],circuits_df["alt"])

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

circuits_selected_df = circuits_df.select(col("circuitId"),col("circuitRef"),col("name"),col("location"),col("country"),col("lat"),col("lng"),col("alt"))

# COMMAND ----------

display(circuits_selected_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Rename the columns as required

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------



# COMMAND ----------

circuits_renamed_df=circuits_selected_df.withColumnRenamed("circuitId","circuit_id") \
.withColumnRenamed("circuitRef","circuit_ref") \
.withColumnRenamed("lat","latitude") \
.withColumnRenamed("lng","longitude") \
.withColumnRenamed("alt","altitude") \
.withColumn("data_source",lit(v_data_source))

# COMMAND ----------

display(circuits_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 4 Add ingestion date column to the dataframe

# COMMAND ----------

circuits_final_df = add_ingestion_date(circuits_renamed_df)

# COMMAND ----------

display(circuits_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 5 Write the data to the datalake as parquet

# COMMAND ----------

circuits_final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/circuits")

# COMMAND ----------

display(spark.read.parquet(f"{processed_folder_path}/circuits"))

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

# MAGIC %md 
# MAGIC #####Get the current path and name of notebook

# COMMAND ----------

dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()

