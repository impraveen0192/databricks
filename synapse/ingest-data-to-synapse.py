# Databricks notebook source
url1 = "jdbc:sqlserver://azure-synapse-worksp.sql.azuresynapse.net:1433;database=dwlab;user=sqladminuser@azure-synapse-worksp;password=Game@2023;encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.sql.azuresynapse.net;loginTimeout=30;"
url

# COMMAND ----------

# Otherwise, set up the Blob storage account access key in the notebook session conf.

spark.conf.set(
  "fs.azure.account.key.sapkformula1dl.blob.core.windows.net",
  "GBdagg9YFyOr+iCABl+Vbwyb76NbI8Crw6G2ChQCgjXxxGcLpcqdrc1pFHaZ8XsVp208ShQNUIJivJaZSj8erg==")

# Get some data from an Azure Synapse table.
df = spark.read \
  .format("com.databricks.spark.sqldw") \
  .option("url", url) \
  .option("tempDir", "wasbs://gold@sapkformula1dl.blob.core.windows.net/temp") \
  .option("forwardSparkAzureStorageCredentials", "true") \
  .option("dbTable", "constructors") \
  .option("maxStrLength",4000)\
  .load()



# COMMAND ----------

df = spark.read.parquet("/mnt/sapkformula1dl/processed/circuits")


# COMMAND ----------

df.write.mode("overwrite").saveAsTable("TBL_CIRCUITS")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from TBL_CIRCUITS

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Otherwise, set up the Blob storage account access key in the notebook session conf.
# MAGIC SET fs.azure.account.key.sapkformula1dl.blob.core.windows.net="GBdagg9YFyOr+iCABl+Vbwyb76NbI8Crw6G2ChQCgjXxxGcLpcqdrc1pFHaZ8XsVp208ShQNUIJivJaZSj8erg==";
# MAGIC 
# MAGIC -- Read data using SQL.
# MAGIC 
# MAGIC 
# MAGIC -- Write data using SQL.
# MAGIC -- Create a new table, throwing an error if a table with the same name already exists:
# MAGIC 
# MAGIC CREATE TABLE example_table_in_spark_write
# MAGIC USING com.databricks.spark.sqldw
# MAGIC OPTIONS (
# MAGIC   url 'jdbc:sqlserver://azure-synapse-worksp.sql.azuresynapse.net:1433;database=dwlab;user=sqladminuser@azure-synapse-worksp;password=Game@2023;encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.sql.azuresynapse.net;loginTimeout=30;',
# MAGIC   forwardSparkAzureStorageCredentials 'true',
# MAGIC   dbTable 'test',
# MAGIC   tempDir 'wasbs://gold@sapkformula1dl.blob.core.windows.net/tempdirsql'
# MAGIC )
# MAGIC AS SELECT * FROM TBL_CIRCUITS;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE example_table_in_spark_read
# MAGIC USING com.databricks.spark.sqldw
# MAGIC OPTIONS (
# MAGIC   url 'jdbc:sqlserver://azure-synapse-worksp.sql.azuresynapse.net:1433;database=dwlab;user=sqladminuser@azure-synapse-worksp;password=Game@2023;encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.sql.azuresynapse.net;loginTimeout=30;',
# MAGIC   forwardSparkAzureStorageCredentials 'true',
# MAGIC   dbTable 'constructors',
# MAGIC   tempDir 'wasbs://gold@sapkformula1dl.blob.core.windows.net/tempdirsql'
# MAGIC );

# COMMAND ----------


