# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

races_df =spark.read.parquet(f"{processed_folder_path}/races")

# COMMAND ----------

display(races_df)

# COMMAND ----------

races_filtered_df = races_df.filter("race_year = 2019")

# COMMAND ----------

display(races_filtered_df)

# COMMAND ----------

races_filtered_df1 = races_df.filter(races_df["race_year"]==2019)

# COMMAND ----------

display(races_filtered_df1)

# COMMAND ----------

races_filtered_df2 = races_df.filter(races_df.race_year == "2019")

# COMMAND ----------

display(races_filtered_df2)

# COMMAND ----------

# MAGIC %md
# MAGIC #####Specify multiple filter conditions

# COMMAND ----------

races_filtered_df = races_df.filter("race_year = 2019 and round  <=5")

# COMMAND ----------

display(races_filtered_df)

# COMMAND ----------

races_filtered_df1 = races_df.filter((races_df["race_year"] == 2019) & (races_df["round"] <= 3))

# COMMAND ----------

display(races_filtered_df1)

# COMMAND ----------

races_filtered_df2 = races_df.filter((races_df.race_year == 2019) & (races_df.round <= 10))

# COMMAND ----------

display(races_filtered_df2)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### filter and wgere can be used as synonyms

# COMMAND ----------

races_filtered_df3 = races_df.where("race_year = 2018")

# COMMAND ----------

display(races_filtered_df3)

# COMMAND ----------

races_df.where((races_df.race_year == 2018) & (races_df.round == 1))

# COMMAND ----------

display(races_df)

# COMMAND ----------


