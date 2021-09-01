# Databricks notebook source
#Unmount
#dbutils.fs.unmount("/mnt/pkformula1dl/raw")
#dbutils.fs.unmount("/mnt/pkformula1dl/processed")
#dbutils.fs.unmount("/mnt/pkformula1dl/gold")


# COMMAND ----------

storage_account_name = "pkmlops" # Update the sturage account name

# COMMAND ----------

dbutils.secrets.help()


# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.list("databricks-storageaccount-accesskey")
#dbutils.secrets.listScopes

# COMMAND ----------

dbutils.secrets.get(scope="Secret-scope-pkmlops", key = "pkmlops-access-key")

# COMMAND ----------

##But if we want we can print the values 
for x in dbutils.secrets.get(scope="formula1-databricks-secret-scope", key = "databricks-app-client-id"):
  print (x)

# COMMAND ----------

# MAGIC %md
# MAGIC Above only restricts the accidential exposing of values to the public. It will not restriect those people who has the access to the Databricks workspace itself . for that we need to do aditioanl set up.

# COMMAND ----------


client_id = dbutils.secrets.get(scope="formula1-databricks-secret-scope", key = "databricks-app-client-id") # This is not good use secrets(batabricks backed secrent scope ) or azure keyvault Secret Scope
tenant_id = dbutils.secrets.get(scope="formula1-databricks-secret-scope", key = "databricks-app-tenant-id")
client_secret = dbutils.secrets.get(scope="formula1-databricks-secret-scope", key = "databricks-aap-client-secret")

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type":"org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id":f"{client_id}",
           "fs.azure.account.oauth2.client.secret":f"{client_secret}",
           "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"
  
 
}

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

mount_adls("raw")

# COMMAND ----------

mount_adls("processed")

# COMMAND ----------

#use above function to mount the gold container 
#mount_adls("gold")

# COMMAND ----------

dbutils.fs.ls("/mnt/")

# COMMAND ----------

container_name = "bronze"
storage_account_name= "staedwsbxcurtest"
storage_account_name
dbutils.fs.mount(
    source = f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net",
    mount_point = f"/mnt/{storage_account_name}/{container_name}",
    extra_configs = 
    {f"fs.azure.account.key.{storage_account_name}.blob.core.windows.net":dbutils.secrets.get(scope = scope_name, key = scope_key)})
print(f"{storage_account_name}/{container_name} mounted")

# COMMAND ----------

dbutils.fs.unmount("/mnt/sapkformula1dl/raw")

# COMMAND ----------


