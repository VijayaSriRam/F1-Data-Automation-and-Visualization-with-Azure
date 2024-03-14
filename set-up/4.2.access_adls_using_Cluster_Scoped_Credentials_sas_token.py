# Databricks notebook source
# MAGIC %md
# MAGIC ####Access Azure Data Lake using Cluster Scoped Credentials
# MAGIC - 1: Set the spark config fs.azure.account.key in the cluster
# MAGIC - 2: List files from demo container
# MAGIC - 3: Read data from circuits.csv file

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.dbcf1dl.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.dbcf1dl.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@dbcf1dl.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@dbcf1dl.dfs.core.windows.net"))