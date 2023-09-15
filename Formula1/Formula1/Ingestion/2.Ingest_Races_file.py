# Databricks notebook source
# MAGIC %md
# MAGIC ###Ingest Races.csv file

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/formula1dlldatabrick/raw

# COMMAND ----------

dbutils.widgets.text("p_data_source","")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %run  "../Include/configuration"

# COMMAND ----------

# MAGIC %run  "../Include/common_function"

# COMMAND ----------

# MAGIC %md
# MAGIC ####Step 1 - Read the CSV file using the spark dataframe reader

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,StringType,DoubleType

# COMMAND ----------

races_schema = StructType(fields=[StructField("raceId",IntegerType(),False),
                                     StructField("year",StringType(),True),
                                     StructField("round",StringType(),True),
                                     StructField("circuitId",IntegerType(),True),
                                     StructField("name",StringType(),True),
                                     StructField("date",DoubleType(),True),
                                     StructField("time",DoubleType(),True),
                                     StructField("url",StringType(),True), 
]) 

# COMMAND ----------

races_df = spark.read \
    .option("header", True) \
    .schema(races_schema) \
    .csv(f"{raw_folder_path}/races.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Step 2 - Add Ingestion data and race_timestamp to the dataframe

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, to_timestamp,concat,col,lit

# COMMAND ----------

races_with_timestamp_df = races_df.withColumn("ingestion_date", current_timestamp()) \
                                  .withColumn("race_timestamp", to_timestamp(concat(col('date')), 'yyyy-MM-dd HH:mm:ss')) \
                                  .withColumn("data_source",lit(v_data_source))
                                

# COMMAND ----------

# MAGIC %md
# MAGIC ####Step 3 - Select only the column required

# COMMAND ----------

races_selected_df = races_with_timestamp_df.select(col('raceId').alias('race_id'),col('year').alias('race_year'),col('round'),col('circuitId').alias('circuit_id'),col('name'),col('ingestion_date'),col('race_timestamp'))

# COMMAND ----------

display(races_selected_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Step 4 - Write data to DataLake as Parquet

# COMMAND ----------

races_selected_df.write.mode("overwrite").partitionBy('race_year').format("parquet").saveAsTable("f1_processed.races")

# COMMAND ----------

dbutils.notebook.exit("Success")