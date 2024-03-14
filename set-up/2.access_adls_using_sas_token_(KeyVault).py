# Databricks notebook source
# MAGIC %md
# MAGIC ####Access Azure Data Lake using SAS Token
# MAGIC - 1: Set the spark config for SAS Token
# MAGIC - 2: List files from demo container
# MAGIC - 3: Read data from circuits.csv file

# COMMAND ----------

f1_dl_sas_token = dbutils.secrets.get(scope = 'f1-scope', key = 'f1-dl-sas-token')

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.dbcf1dl.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.dbcf1dl.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.dbcf1dl.dfs.core.windows.net", f1_dl_sas_token)

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@dbcf1dl.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@dbcf1dl.dfs.core.windows.net"))
