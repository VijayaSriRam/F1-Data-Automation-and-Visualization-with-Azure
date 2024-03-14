# Databricks notebook source
# MAGIC %md
# MAGIC ####Ingest races.csv file

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
# MAGIC #####Step-1: Read the csv file using Spark DataFrame Reader

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, DateType, StringType

# COMMAND ----------

races_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                  StructField("year", IntegerType(), True),
                                  StructField("round", IntegerType(), True),
                                  StructField("circuitId", IntegerType(), True),
                                  StructField("name", StringType(), True),
                                  StructField("date", DateType(), True),
                                  StructField("time", StringType(), True),
                                  StructField("url", StringType(), True)
])

# COMMAND ----------

races_df = spark.read \
.option("header", True) \
.schema(races_schema) \
.csv(f"{raw_folder_path}/{v_file_date}/races.csv")

# COMMAND ----------

display(races_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step-2: Add ingestion data and race_timestamp to the dataframe
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import lit, to_timestamp, concat, col, current_timestamp

# COMMAND ----------

races_with_timestamp_df = races_df.withColumn("race_timestamp", to_timestamp(concat(col('date'), lit(' '), col('time')), 'yyyy-MM-dd HH:mm:ss'))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step-3: Selecting only the required Columns

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

races_sel_columns_df = races_with_timestamp_df.select(col("raceId"), col("year"), col("round"), col("circuitId"), col("name"), col("race_timestamp"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step-4: Renaming the columns as needed

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

races_ingestion = add_ingestion_date(races_sel_columns_df)

# COMMAND ----------

races_final_df = races_ingestion.withColumnRenamed("raceId", "race_id") \
.withColumnRenamed("year", "race_year") \
.withColumnRenamed("circuitId", "circuit_id") \
.withColumn("data_source", lit(v_data_source)) \
.withColumn("file_date", lit(v_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step-5: Write data to DataLake as Parquet

# COMMAND ----------

races_final_df.write.mode("overwrite").partitionBy("race_year").format("delta").saveAsTable("f1_processed.races")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed.races;

# COMMAND ----------

dbutils.notebook.exit("Success")