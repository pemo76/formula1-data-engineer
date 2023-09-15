# Databricks notebook source
# MAGIC %md
# MAGIC ###Ingest Laptimes.json file

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

lap_times_schema = StructType(fields=[StructField("raceId",IntegerType(),False),
                                 StructField("driverId",IntegerType(),True),
                                 StructField("lap",IntegerType(),True),
                                 StructField("position",IntegerType(),True),
                                 StructField("time",StringType(),True),
                                 StructField("milliseconds",IntegerType(),True),
])

# COMMAND ----------

lap_times_df = spark.read \
.schema(lap_times_schema) \
.json(f'{raw_folder_path}/lap_times')

# COMMAND ----------

# MAGIC %md
# MAGIC ####Step 2 - Rename Column and add new column

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

final_df1 = lap_times_df.withColumnRenamed("driverId","driver_id") \
                                   .withColumnRenamed("raceId","race_Id") \
                                   .withColumn("data_source",lit(v_data_source))

# COMMAND ----------

final_df = add_ingestion_date(final_df1)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Step 4 - Write data in data lake storage

# COMMAND ----------

final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.laptimes")

# COMMAND ----------

dbutils.notebook.exit("Success")