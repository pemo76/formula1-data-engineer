# Databricks notebook source
# MAGIC %md
# MAGIC ###Ingest Constructors.json file

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

constructors_schema = "constructorId INT,constructorRef STRING,name STRING,nationality STRING,url STRING"

# COMMAND ----------

constructor_df = spark.read \
.schema(constructors_schema) \
.json(f"{raw_folder_path}/constructors.json")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Step 2 - Drop unwanted column from the dataframe

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

constructor_drop_df = constructor_df.drop(col('url'))

# COMMAND ----------

# MAGIC %md
# MAGIC ####Step 3 - Rename Column and add ingested date

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

constructor_final_df1 = constructor_drop_df.withColumnRenamed("constructorId","constructor_id") \
                                          .withColumnRenamed("constructorRef","constructor_ref") \
                                          .withColumn("data_source",lit(v_data_source))

# COMMAND ----------

constructor_final_df = add_ingestion_date(constructor_final_df1)

# COMMAND ----------

display(constructor_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Step 4 - Write data in data lake storage

# COMMAND ----------

constructor_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.constructors")

# COMMAND ----------

dbutils.notebook.exit("Success")