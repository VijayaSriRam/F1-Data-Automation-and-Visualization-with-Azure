# Databricks notebook source
raw_folder_path = '/mnt/dbcf1dl/raw'
processed_folder_path = '/mnt/dbcf1dl/processed'
presentation_folder_path = '/mnt/dbcf1dl/presentation'

# COMMAND ----------

# #Incase due to student-subscription or others; we cannot mount the DL. We need to use abfss protocols.
# raw_folder_path = 'abfss://raw@dbcf1dl.dfs.core.windows.net'
# processed_folder_path = 'abfss://processed@dbcf1dl.dfs.core.windows.net'
# presentation_folder_path = 'abfss://presentation@dbcf1dl.dfs.core.windows.net'
