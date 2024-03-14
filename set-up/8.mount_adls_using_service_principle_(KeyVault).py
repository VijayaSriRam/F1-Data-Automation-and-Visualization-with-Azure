# Databricks notebook source
# MAGIC %md
# MAGIC ####Mount Azure Data Lake using Service Principle
# MAGIC - 1: Get client_id, teneant_id and client_secret from key vault
# MAGIC - 2: Set spark config with app/client id, directory/tenant id & secret
# MAGIC - 3: Call file system utility mount to the storage
# MAGIC - 4: Explore other file system utilities related to mount (list all mounts, unmount)

# COMMAND ----------

client_id = dbutils.secrets.get(scope='f1-scope', key='f1-dl-client-id')
tenant_id = dbutils.secrets.get(scope='f1-scope', key='f1-dl-tenant-id')
client_secret = dbutils.secrets.get(scope='f1-scope', key='f1-dl-client-secret')

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# COMMAND ----------

dbutils.fs.mount(
  source = "abfss://demo@dbcf1dl.dfs.core.windows.net/",
  mount_point = "/mnt/dbcf1dl/demo",
  extra_configs = configs)

# COMMAND ----------

display(dbutils.fs.ls('/mnt/dbcf1dl/demo'))

# COMMAND ----------

display(spark.read.csv("/mnt/dbcf1dl/demo/circuits.csv"))

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

#display(dbutils.fs.unmount('/mnt/dbcf1dl/demo'))