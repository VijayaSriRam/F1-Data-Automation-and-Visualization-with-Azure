# Databricks notebook source
# MAGIC %md
# MAGIC #### Ingest drivers.json file

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

from pyspark.sql.types import StructField, StructType, IntegerType, StringType, DateType

# COMMAND ----------

name_schema = StructType(fields=[StructField("forename", StringType(), True),
                                 StructField("surname", StringType(), True)
                                 ])

# COMMAND ----------

driver_schema = StructType(fields=[StructField("driverId", IntegerType(), False),
                                   StructField("driverRef", StringType(), True),
                                   StructField("number", IntegerType(), True),
                                   StructField("code", StringType(), True),
                                   StructField("name", name_schema),
                                   StructField("dob", DateType(), True),
                                   StructField("nationality", StringType(), True),
                                   StructField("url", StringType(), True),
                                   ])

# COMMAND ----------

driver_df = spark.read \
.schema(driver_schema) \
.json(f"{raw_folder_path}/{v_file_date}/drivers.json")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step-2: Rename columns and add new columns
# MAGIC - 1: driverId remaned to driver_id
# MAGIC - 2: driverRef renamed to driver_ref
# MAGIC - 3: ingestion data added
# MAGIC - 4: name added with concatenation of forename and surname

# COMMAND ----------

from pyspark.sql.functions import col, concat, current_timestamp, lit

# COMMAND ----------

driver_ingestion = add_ingestion_date(driver_df)

# COMMAND ----------

driver_with_columns_df = driver_ingestion.withColumnRenamed("driverId", "driver_id") \
                                .withColumnRenamed("driverRef", "driver_ref") \
                                .withColumn("name", concat(col("name.forename"), lit(' '), col("name.surname"))) \
                                .withColumn("data_source", lit(v_data_source)) \
                                .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step-3: Drop the unwanted columns
# MAGIC - 1: name.forename
# MAGIC - 2: name.surname
# MAGIC - 3: url

# COMMAND ----------

driver_final_df = driver_with_columns_df.drop(col("url"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step-4: Write the output to the processed contained in parquet format

# COMMAND ----------

driver_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.drivers")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed.drivers;

# COMMAND ----------

dbutils.notebook.exit("Success")