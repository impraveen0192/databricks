# Databricks notebook source
storage_account_name = "sapkformula1dl"
client_id = "8b6cbc4f-7b09-49df-b25f-baecdd318ae0" # This is not good use secrets(batabricks backed secrent scope ) or azure keyvault Secret Scope
tenant_id = "595336d3-2ae8-4525-81d5-713d46ae53d6"
client_secret = "~D00y7lg5Wz3Zu82b~_BEJ0.8Sw2W7FKaJ"

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type":"org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id":f"{client_id}",
           "fs.azure.account.oauth2.client.secret":f"{client_secret}",
           "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"
  
 
}

# COMMAND ----------

container_name = "raw"
dbutils.fs.mount(
  source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
  mount_point = f"/mnt/{storage_account_name}/{container_name}",
  extra_configs=configs)

# COMMAND ----------

dbutils.fs.ls('/mnt/sapkformula1dl/')

# COMMAND ----------

dbutils.fs.mounts()

# COMMAND ----------

container_name = "processed"
dbutils.fs.mount(
  source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
  mount_point = f"/mnt/{storage_account_name}/{container_name}",
  extra_configs=configs)

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

#use above function to mount the gold container 
mount_adls("gold")

# COMMAND ----------

dbutils.fs.ls("/mnt/")

# COMMAND ----------

dbutils.fs.unmount("/mnt/sapkformula1dl/raw")

# COMMAND ----------


