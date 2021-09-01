# Databricks notebook source
# MAGIC %md
# MAGIC ###Below function provides the list of scopes. Each scopes represents the scope for a perticular storage account

# COMMAND ----------

dbutils.secrets.listScopes()


# COMMAND ----------

# MAGIC %md
# MAGIC ###Below function provides the Key of the scope. Pass the required scope from previous step.

# COMMAND ----------

dbutils.secrets.list("Secret-Scope-edwdevarmdlsuw2101")


# COMMAND ----------

# MAGIC %md
# MAGIC ###Update the below variables with scope name and the scope key for the storage account whose's container needs to be mounted

# COMMAND ----------

scope_name = "Secret-Scope-edwdevarmdlsuw2101"
scope_key = "Key-edwdevarmdlsuw2101"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Mount function It takes Two variables 
# MAGIC ###container_name and storage_account_name

# COMMAND ----------

def mount_adls(container_name,storage_account_name):
  dbutils.fs.mount(
    source = f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net",
    mount_point = f"/mnt/{storage_account_name}/{container_name}",
    extra_configs = 
    {f"fs.azure.account.key.{storage_account_name}.blob.core.windows.net":dbutils.secrets.get(scope = scope_name, key = scope_key)})
  print(f"{storage_account_name}/{container_name} mounted")


# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Unmount function It takes two variables 
# MAGIC ###container_name and storage_account_name

# COMMAND ----------

def unmount_adls(container_name,storage_account_name):
  dbutils.fs.unmount(f"/mnt/{storage_account_name}/{container_name}")


# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Calling mount function using "container name" and the "storage account" name

# COMMAND ----------

mount_adls("primavera","edwdevarmdlsuw2101")

# COMMAND ----------

# MAGIC %md
# MAGIC ##4. Calling unmount function

# COMMAND ----------

unmount_adls("flatfile","edwdevarmdlsuw2101")

# COMMAND ----------

#Test read
df = spark.read.parquet(f"/mnt/edwdevarmdlsuw2201/raw/flatfile/downstream/petropass/ccsmast.csv.20180717040756.parquet")

# COMMAND ----------

#test write
df.write.csv("/mnt/edwdevarmdlsuw2201/raw/flatfile/downstream/ppast")

# COMMAND ----------


