# Databricks notebook source
# MAGIC %md
# MAGIC ####Access Azure Data Lake using Access Keys
# MAGIC - 1: Set the spark config fs.azure.account.key
# MAGIC - 2: List files from demo container
# MAGIC - 3: Read data from circuits.csv file

# COMMAND ----------

f1_dl_account_key = dbutils.secrets.get(scope = 'f1-scope', key = 'f1-dl-account-key')

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.dbcf1dl.dfs.core.windows.net",
    f1_dl_account_key)

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@dbcf1dl.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@dbcf1dl.dfs.core.windows.net"))
