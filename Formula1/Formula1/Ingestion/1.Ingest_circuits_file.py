# Databricks notebook source
# MAGIC %md
# MAGIC ###Ingest Circuit.csv file

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

# MAGIC %run "../Include/common_function"

# COMMAND ----------

# MAGIC %md
# MAGIC ####Step 1 - Read the CSV file using the spark dataframe reader

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,StringType,DoubleType

# COMMAND ----------

circuits_schema = StructType(fields=[StructField("circuitId",IntegerType(),False),
                                     StructField("circuitRef",StringType(),True),
                                     StructField("name",StringType(),True),
                                     StructField("location",StringType(),True),
                                     StructField("country",StringType(),True),
                                     StructField("lat",DoubleType(),True),
                                     StructField("lang",DoubleType(),True),
                                     StructField("alt",IntegerType(),True),
                                     StructField("url",StringType(),True), 
]) 

# COMMAND ----------

circuit_df = spark.read \
    .option("header", True) \
    .schema(circuits_schema) \
    .csv(f"{raw_folder_path}/circuits.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Step 2 select only the required column

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

circuit_selected_df = circuit_df.select(col("circuitId"),col("circuitRef"),col("name"),col("location"),col("country"),col("lat"),col("lang"),col("alt"))

# COMMAND ----------

# MAGIC %md
# MAGIC ####Step 3 - Reaname the cloumn as required

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

circuits_renamed_df = circuit_selected_df.withColumnRenamed("circuitId","circuit_id") \
    .withColumnRenamed("circuitRef","circuit_ref") \
    .withColumnRenamed("lat","latitude") \
    .withColumnRenamed("lang","longitude") \
    .withColumnRenamed("alt","altitude") \
    .withColumn("data_source",lit(v_data_source))

# COMMAND ----------

# MAGIC %md
# MAGIC ####Step 4 - Add Ingestion data to the dataframe

# COMMAND ----------

circuit_final_df = add_ingestion_date(circuits_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Step 5 - Write data to DataLake as Parquet

# COMMAND ----------

circuit_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.circuits")

# COMMAND ----------

dbutils.notebook.exit("Success")