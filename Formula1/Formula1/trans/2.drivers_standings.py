# Databricks notebook source
# MAGIC %md
# MAGIC ####Produce Driver Standing

# COMMAND ----------

# MAGIC %run "../Include/configuration"

# COMMAND ----------

race_result_df = spark.read.parquet(f"{presentation_folder_path}/race_result")

# COMMAND ----------

from pyspark.sql.functions import sum,when,col,count

driver_standing_df = race_result_df \
    .groupBy("race_year","drive_name","drive_nationality","team") \
    .agg(sum("points").alias("total_points"),
    count(when(col("position")== 1, True)).alias("wins"))

# COMMAND ----------

display(driver_standing_df.filter("race_year = 2020"))

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc,rank,asc

driver_rank_spec = Window.partitionBy("race_year").orderBy(desc("total_points"),desc("wins"))
final_df = driver_standing_df.withColumn("rank",rank().over(driver_rank_spec))

# COMMAND ----------

display(final_df)

# COMMAND ----------

final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_presentation.driver_standings")