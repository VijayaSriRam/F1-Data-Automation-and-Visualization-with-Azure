# Databricks notebook source
# MAGIC %md
# MAGIC ####Mount Azure Data Lake for the Project

# COMMAND ----------

def mount_adls(storage_account_name, container_name):
    #Get secrets form the key vault
    client_id = dbutils.secrets.get(scope='f1-scope', key='f1-dl-client-id')
    tenant_id = dbutils.secrets.get(scope='f1-scope', key='f1-dl-tenant-id')
    client_secret = dbutils.secrets.get(scope='f1-scope', key='f1-dl-client-secret')

    #Set spark config
    configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}
    
    #Unmount the mount point if it already exists
    if any(mount.mountPoint == f"/mnt/{storage_account_name}/{container_name}" for mount in dbutils.fs.mounts()):
        dbutils.fs.unmount(f"/mnt/{storage_account_name}/{container_name}")

    #Mount the storage using dbfs utility
    dbutils.fs.mount(
        source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
        mount_point = f"/mnt/{storage_account_name}/{container_name}",
        extra_configs = configs)
    
    #List of mounts
    display(dbutils.fs.mounts())

# COMMAND ----------

# MAGIC %md
# MAGIC #### Mount Raw Container

# COMMAND ----------

mount_adls('dbcf1dl', 'raw')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Mount Processed Container

# COMMAND ----------

mount_adls('dbcf1dl', 'processed')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Mount Presentation Container

# COMMAND ----------

mount_adls('dbcf1dl', 'presentation')

# COMMAND ----------

#display(dbutils.fs.unmount('/mnt/dbcf1dl/demo'))