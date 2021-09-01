# Databricks notebook source
# MAGIC %md
# MAGIC ##### Aggergation
# MAGIC ####1. Simple aggregation
# MAGIC ####2. Grouped aggregations
# MAGIC ####3. Windows aggregations
# MAGIC ####4. Apply aggregations to tyhe dataframe

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

races_results_df = spark.read.parquet(f"{presentation_folder_path}/races_results")

# COMMAND ----------

display(races_results_df)

# COMMAND ----------

demo_df = races_results_df.filter("race_year == 2020")

# COMMAND ----------

display(demo_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ####1. Simple aggregation

# COMMAND ----------

from pyspark.sql.functions import count,countDistinct,sum

# COMMAND ----------

demo_df.count()

# COMMAND ----------

demo_df.select(count("*")).show()

# COMMAND ----------

demo_df.select(countDistinct("race_name")).show()

# COMMAND ----------

demo_df.filter("drivers_name = 'Lewis Hamilton'").select(sum("points"),countDistinct("race_name"))\
.withColumnRenamed("sum(points)", "total_points")\
.withColumnRenamed("count(DISTINCT race_name)","number_of_races")\
.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ####2 Grouped aggregations

# COMMAND ----------

# MAGIC %mdGrouped aggregations

# COMMAND ----------

demo_df\
.groupBy("drivers_name")\
.sum("points")\
.show()

# COMMAND ----------

demo_df\
.groupBy("drivers_name")\
.agg(sum("points").alias("total_points"),countDistinct("race_name").alias("number_of_races"))\
.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ####3 Windows function

# COMMAND ----------

demo_df = races_results_df.filter("race_year in (2019,2020)")

# COMMAND ----------

display(demo_df)

# COMMAND ----------

demo_grouped_df = demo_df\
.groupBy("race_year","drivers_name")\
.agg(sum("points").alias("total_points"),countDistinct("race_name").alias("number_of_races"))


# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import  desc,rank

# COMMAND ----------

driverRankSpec = Window.partitionBy("race_year").orderBy(desc("total_points"))
demo_grouped_df.withColumn("rank",rank().over(driverRankSpec)).show()

# COMMAND ----------



# COMMAND ----------


