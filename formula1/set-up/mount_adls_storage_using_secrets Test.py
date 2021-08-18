# Databricks notebook source
#Unmount
#dbutils.fs.unmount("/mnt/pkformula1dl/raw")
#dbutils.fs.unmount("/mnt/pkformula1dl/processed")
#dbutils.fs.unmount("/mnt/pkformula1dl/gold")


# COMMAND ----------

dbutils.secrets.help()


# COMMAND ----------

 dbutils.secrets.listScopes()
  

# COMMAND ----------

dbutils.secrets.list("Test_Service_principal_APP")


# COMMAND ----------

dbutils.secrets.get(scope="Test_Service_principal_APP", key = "client-id")

# COMMAND ----------

# MAGIC %md
# MAGIC Above only restricts the accidential exposing of values to the public. It will not restriect those people who has the access to the Databricks workspace itself . for that we need to do aditioanl set up.

# COMMAND ----------

storage_account_name = "sapkformula1dl" # Update the sturage account name
client_id = dbutils.secrets.get(scope="Test_Service_principal_APP", key = "client-id") 
tenant_id = dbutils.secrets.get(scope="Test_Service_principal_APP", key = "tenant-id")
client_secret = dbutils.secrets.get(scope="Test_Service_principal_APP", key = "seceret-key")

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type":"org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id":f"{client_id}",
           "fs.azure.account.oauth2.client.secret":f"{client_secret}",
           "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"
  
 
}

# COMMAND ----------

Age = '40'
Salary = '5M CAD'
print(f"my name is praveen and I am  {Age} year old  My {Salary} is  Salary")

# COMMAND ----------

# MAGIC %md
# MAGIC Create function to mount the containers

# COMMAND ----------

def mount_adls(container_name):
  dbutils.fs.mount(
    source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
    mount_point = f"/mnt/{storage_account_name}/{container_name}",
    extra_configs=configs)

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/sapkformula1dl/

# COMMAND ----------

gold_df = spark.read.option("header",True).option("inferSchema",True).csv("/mnt/sapkformula1dl/raw/circuits v2.csv")

# COMMAND ----------

display(gold_df)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

gold_drop_df = gold_df.withColumn("Ingestion_Time",current_timestamp()).drop("url")

# COMMAND ----------

display(gold_drop_df)

# COMMAND ----------


gold_drop_df.write.format("delta").mode("overwrite").save("/mnt/sapkformula1dl/gold/circuits1")

# COMMAND ----------

display(spark.read.format("delta").option("versionAsOf",2 ).load("/mnt/sapkformula1dl/gold/circuits1"))

# COMMAND ----------


