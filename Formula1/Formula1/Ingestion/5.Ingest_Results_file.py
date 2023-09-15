# Databricks notebook source
# MAGIC %md
# MAGIC ###Ingest Results.json file

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

from pyspark.sql.types import StructType,StructField,IntegerType,StringType,FloatType

# COMMAND ----------

results_schema = StructType(fields=[StructField("resultId",IntegerType(),False),
                                 StructField("raceId",IntegerType(),True),
                                 StructField("driverId",IntegerType(),True),
                                 StructField("constructorId",IntegerType(),True),
                                 StructField("number",IntegerType(),True),
                                 StructField("grid",IntegerType(),True),
                                 StructField("position",IntegerType(),True),
                                 StructField("positionText",StringType(),True),
                                 StructField("positionOrder",IntegerType(),True),
                                 StructField("points",FloatType(),True),
                                 StructField("laps",IntegerType(),True),
                                 StructField("time",StringType(),True),
                                 StructField("milliseconds",IntegerType(),True),
                                 StructField("fastestlap",IntegerType(),True),
                                 StructField("rank",IntegerType(),True),
                                 StructField("fastestLapTime",StringType(),True),
                                 StructField("fastestLapSpeed",FloatType(),True),
                                 StructField("statusId",StringType(),True)
])

# COMMAND ----------

result_df = spark.read \
.schema(results_schema) \
.json(f'{raw_folder_path}/results.json')

# COMMAND ----------

# MAGIC %md
# MAGIC ####Step 2 - Rename Column and add new column

# COMMAND ----------

from pyspark.sql.functions import col,concat,lit

# COMMAND ----------

results_with_column_df1 = result_df.withColumnRenamed("resultId","result_id") \
                                   .withColumnRenamed("raceId","race_id") \
                                   .withColumnRenamed("driverId","driver_id") \
                                   .withColumnRenamed("constructorId","constructor_id") \
                                   .withColumnRenamed("positionText","position_text") \
                                   .withColumnRenamed("positionOrder","position_order") \
                                   .withColumnRenamed("fastestLap","fastest_lap") \
                                   .withColumnRenamed("fastestLapTime","fastest_lap_time") \
                                   .withColumnRenamed("fastestLapSpeed","fastest_lap_speed") \
                                   .withColumn("data_source",lit(v_data_source))

# COMMAND ----------

results_with_column_df = add_ingestion_date(results_with_column_df1)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Step 3 - Drop unwanted column from the dataframe

# COMMAND ----------

result_final_df = results_with_column_df.drop(col("statusId"))

# COMMAND ----------

# MAGIC %md
# MAGIC ####Step 4 - Write data in data lake storage

# COMMAND ----------

result_final_df.write.mode("overwrite").partitionBy("race_id").format("parquet").saveAsTable("f1_processed.results")

# COMMAND ----------

dbutils.notebook.exit("Success")