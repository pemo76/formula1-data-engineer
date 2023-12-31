# Databricks notebook source
# MAGIC %md
# MAGIC ####Read all required dataset

# COMMAND ----------

# MAGIC %run "../Include/configuration"

# COMMAND ----------

drivers_df = spark.read.parquet(f"{processed_folder_path}/drivers") \
    .withColumnRenamed("number","driver_number") \
    .withColumnRenamed("name","drive_name") \
    .withColumnRenamed("nationality","drive_nationality")

# COMMAND ----------

constructors_df = spark.read.parquet(f"{processed_folder_path}/constructor") \
    .withColumnRenamed("name","team")

# COMMAND ----------

circuits_df = spark.read.parquet(f"{processed_folder_path}/circuits") \
    .withColumnRenamed("location","circuit_location")

# COMMAND ----------

races_df = spark.read.parquet(f"{processed_folder_path}/races") \
    .withColumnRenamed("name","race_name") \
    .withColumnRenamed("race_timestamp","race_date")

# COMMAND ----------

results_df = spark.read.parquet(f"{processed_folder_path}/results") \
    .withColumnRenamed("time","race_time")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Join circuit to race

# COMMAND ----------

race_circuit_df = races_df.join(circuits_df , races_df.circuit_id == circuits_df.circuit_id , "inner") \
    .select(races_df.race_id, races_df.race_year,races_df.race_name,races_df.race_date,circuits_df.circuit_location)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Join results to all other dataframe

# COMMAND ----------

race_result_df = results_df.join(race_circuit_df, results_df.race_id == race_circuit_df.race_id) \
                          .join(drivers_df, results_df.driver_id == drivers_df.driver_id) \
                          .join(constructors_df, results_df.constructor_id == constructors_df.constructor_id)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

final_df = race_result_df.select("race_year","race_name","race_date","circuit_location","drive_name","driver_number","drive_nationality","team","grid","fastest_lap","race_time","position","points") \
    .withColumn("current_date",current_timestamp())

# COMMAND ----------

display(final_df.filter("race_year == 2020 and race_name =='Abu Dhabi Grand Prix'").orderBy(final_df.points.desc()))

# COMMAND ----------

final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_presentation.race_result")