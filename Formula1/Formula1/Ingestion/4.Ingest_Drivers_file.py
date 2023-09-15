# Databricks notebook source
# MAGIC %md
# MAGIC ###Ingest Drivers.json file

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

from pyspark.sql.types import StructType,StructField,IntegerType,StringType,DateType

# COMMAND ----------

name_schema = StructType(fields=[StructField("forename",StringType(),True),
                                 StructField("surname",StringType(),True)
])

# COMMAND ----------

drivers_schema = StructType(fields=[StructField("driverId",IntegerType(),False),
                                 StructField("driverRef",StringType(),True),
                                 StructField("number",IntegerType(),True),
                                 StructField("code",StringType(),True),
                                 StructField("name",name_schema),
                                 StructField("dob",DateType(),True),
                                 StructField("nationality",StringType(),True),
                                 StructField("url",StringType(),True)
])

# COMMAND ----------

drivers_df = spark.read \
.schema(drivers_schema) \
.json(f'{raw_folder_path}/drivers.json')

# COMMAND ----------

# MAGIC %md
# MAGIC ####Step 2 - Rename Column and add new column

# COMMAND ----------

from pyspark.sql.functions import col,concat,current_timestamp,lit

# COMMAND ----------

drivers_with_column_df1 = drivers_df.withColumnRenamed("driverId","driver_id") \
                                   .withColumnRenamed("driverRef","driver_Ref") \
                                   .withColumn("name",concat(col("name.forename"),lit(" "),col("name.surname"))) \
                                   .withColumn("data_source",lit(v_data_source))

# COMMAND ----------

drivers_with_column_df = add_ingestion_date(drivers_with_column_df1)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Step 3 - Drop unwanted column from the dataframe

# COMMAND ----------

drivers_final_df = drivers_with_column_df.drop(col("url"))

# COMMAND ----------

display(drivers_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Step 4 - Write data in data lake storage

# COMMAND ----------

drivers_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.drivers")

# COMMAND ----------

dbutils.notebook.exit("Success")