# Databricks notebook source
# MAGIC %md
# MAGIC ####Access Azure Data Lake using Service Principle
# MAGIC - 1: Register AAD / Service Principle
# MAGIC - 2: Generate secret / password for the app
# MAGIC - 3: Set spark config with app/client id, directory/tenant id & secret
# MAGIC - 4: Assign role 'Storage Blob Data Container' to the DL

# COMMAND ----------

client_id = dbutils.secrets.get(scope='f1-scope', key='f1-dl-client-id')
tenant_id = dbutils.secrets.get(scope='f1-scope', key='f1-dl-tenant-id')
client_secret = dbutils.secrets.get(scope='f1-scope', key='f1-dl-client-secret')

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.dbcf1dl.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.dbcf1dl.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.dbcf1dl.dfs.core.windows.net", client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.dbcf1dl.dfs.core.windows.net", client_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.dbcf1dl.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@dbcf1dl.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@dbcf1dl.dfs.core.windows.net"))