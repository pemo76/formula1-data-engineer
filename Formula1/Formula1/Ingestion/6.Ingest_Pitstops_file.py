# Databricks notebook source
# MAGIC %md
# MAGIC ###Ingest Pitstops.json file

# COMMAND ----------

dbutils.widgets.text("p_data_source","")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %run  "../Include/configuration"

# COMMAND ----------

# MAGIC %run  "../Include/common_function"

# COMMAND ----------

# MAGIC %md
# MAGIC ####Step 1 - Read the JSON file using the spark dataframe reader

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,StringType

# COMMAND ----------

pit_stops_schema = StructType(fields=[StructField("raceId",IntegerType(),False),
                                 StructField("driverId",IntegerType(),True),
                                 StructField("stop",StringType(),True),
                                 StructField("lap",IntegerType(),True),
                                 StructField("time",StringType(),True),
                                 StructField("duration",StringType(),True),
                                 StructField("milliseconds",IntegerType(),True),
])

# COMMAND ----------

pit_stops_df = spark.read \
.schema(pit_stops_schema) \
.option("multiLine",False) \
.json(f'{raw_folder_path}/pit_stops.json')

# COMMAND ----------

# MAGIC %md
# MAGIC ####Step 2 - Rename Column and add new column

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

pitstops_with_column_df1 = pit_stops_df.withColumnRenamed("driverId","driver_id") \
                                   .withColumnRenamed("raceId","race_Id") \
                                   .withColumn("data_source",lit(v_data_source))

# COMMAND ----------

pitstops_with_column_df = add_ingestion_date(pitstops_with_column_df1)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Step 4 - Write data in data lake storage

# COMMAND ----------

pitstops_with_column_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.pitstops")

# COMMAND ----------

dbutils.notebook.exit("Success")