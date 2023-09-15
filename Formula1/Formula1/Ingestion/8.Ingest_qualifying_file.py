# Databricks notebook source
# MAGIC %md
# MAGIC ###Ingest qualifying folder

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

qualifying_schema = StructType(fields=[StructField("qualifyId",IntegerType(),False),
                                 StructField("raceId",IntegerType(),True),
                                 StructField("driverId",IntegerType(),True),
                                 StructField("constructorId",IntegerType(),True),
                                 StructField("number",IntegerType(),True),
                                 StructField("position",IntegerType(),True),
                                 StructField("q1",StringType(),True),
                                 StructField("q2",StringType(),True),
                                 StructField("q3",StringType(),True),
])

# COMMAND ----------

qualifying_df = spark.read \
.schema(qualifying_schema) \
.option("multiLine", True) \
.json(f'{raw_folder_path}/qualifying')

# COMMAND ----------

# MAGIC %md
# MAGIC ####Step 2 - Rename Column and add new column

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

final_df1 = qualifying_df.withColumnRenamed("qualifyId","qualify_id") \
                                   .withColumnRenamed("driverId","driver_id") \
                                   .withColumnRenamed("raceId","race_Id") \
                                   .withColumn("data_source",lit(v_data_source))

# COMMAND ----------

final_df = add_ingestion_date(final_df1)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Step 4 - Write data in data lake storage

# COMMAND ----------

final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.qualifying")

# COMMAND ----------

dbutils.notebook.exit("Success")