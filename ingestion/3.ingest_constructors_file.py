# Databricks notebook source
# MAGIC %md
# MAGIC #### Ingest constructors.json file

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step-1: Read the JSON file using Spark DF Reader API

# COMMAND ----------

constructor_schema = "constructorId INTEGER, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

constructor_df = spark.read \
.schema(constructor_schema) \
.json(f"{raw_folder_path}/{v_file_date}/constructors.json")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step-2: Drop unwanted columns from the DF

# COMMAND ----------

#Way - 1:
#constructor_dropped_df = constructor_df.drop('url')

# COMMAND ----------

#Way - 2:
#constructor_dropped_df = constructor_df.drop(constructor_df['url'])

# COMMAND ----------

#Way - 3:
from pyspark.sql.functions import col

# COMMAND ----------

constructor_dropped_df = constructor_df.drop(col('url'))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step-3: Rename columns and add ingestion data

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

constructor_ingestion = add_ingestion_date(constructor_dropped_df)

# COMMAND ----------

constructor_final_df = constructor_ingestion.withColumnRenamed("constructorId", "constructor_id") \
                                    .withColumnRenamed("constructorRef", "constructor_ref") \
                                    .withColumn("data_source", lit(v_data_source)) \
                                    .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step-4: Write output to parquet file

# COMMAND ----------

constructor_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.constructors")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed.constructors;

# COMMAND ----------

dbutils.notebook.exit("Success")