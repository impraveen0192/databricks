# Databricks notebook source
storage_account_name = "pkformula1dl"
client_id = "9c9f70f1-5a19-4b30-bdc0-792478630f89" # This is not good use secrets(batabricks backed secrent scope ) or azure keyvault Secret Scope
tenant_id = "cce4b829-1804-453a-85f4-5abeacc01292"
client_secret = "JYZtJ-LeQSa.Ti~60~07nBfOwJ3_99-wJ9"

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

dbutils.fs.ls('/mnt/pkformula1dl/')

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

dbutils.fs.unmount("/mnt/pkformula1dl/gold")
dbutils.fs.unmount("/mnt/pkformula1dl/processed")

# COMMAND ----------


