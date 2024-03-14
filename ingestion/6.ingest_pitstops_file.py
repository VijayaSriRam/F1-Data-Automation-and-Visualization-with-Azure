# Databricks notebook source
# MAGIC %md
# MAGIC #### Ingest pit_stops.json file

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-28")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step-1: Read the JSON file using Spark DF Reader API

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

pit_stops_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                      StructField("driverId", IntegerType(), True),
                                      StructField("stop", StringType(), True),
                                      StructField("lap", IntegerType(), True),
                                      StructField("time", StringType(), True),
                                      StructField("duration", StringType(), True),
                                      StructField("milliseconds", IntegerType(), True),
                                      ])

# COMMAND ----------

pit_stops_df = spark.read \
.schema(pit_stops_schema) \
.option("multiLine", True) \
.json(f"{raw_folder_path}/{v_file_date}/pit_stops.json")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step-2: Rename columns and add new columns
# MAGIC - 1: Rename driverId & raceId to driver_id & race_id
# MAGIC - 2: Add ingestion_date with current_timestamp()

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp, lit

# COMMAND ----------

pit_stops_ingestion = add_ingestion_date(pit_stops_df)

# COMMAND ----------

pit_stops_final_df = pit_stops_ingestion.withColumnRenamed("driverId", "driver_id") \
                                .withColumnRenamed("raceId", "race_id") \
                                .withColumn("data_source", lit(v_data_source)) \
                                .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step-3: Write the output to the processed container in parquet format

# COMMAND ----------

#overwrite_partition(pit_stops_final_df, "f1_processed", "pit_stops", "race_id")

# COMMAND ----------

merge_condition = "tgt.race_id = src.race_id and tgt.driver_id = src.driver_id and tgt.stop = src.stop and tgt.race_id = src.race_id"
merge_delta_data(pit_stops_final_df, "f1_processed", "pit_stops", processed_folder_path, merge_condition, "race_id")

# COMMAND ----------

#pit_stops_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.pit_stops")

# COMMAND ----------

#display(spark.read.parquet(f"{processed_folder_path}/pit_stops"))

# COMMAND ----------

# %sql
# select * from f1_processed.pit_stops;

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from f1_processed.pit_stops;

# COMMAND ----------

# %sql
# drop table f1_processed.pit_stops;