# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Azure Data Lake Using Access Key
# MAGIC 1.set the spark config fs.azure.account.key
# MAGIC
# MAGIC 2.List files form demo container
# MAGIC
# MAGIC 3.Read data from circuit.csv file

# COMMAND ----------

formula1_access_key1 = dbutils.secrets.get(scope = 'formula1_scope',key = 'formula1-access-key')

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.formula1dlldatabrick.dfs.core.windows.net",
    formula1_access_key1)

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1dlldatabrick.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1dlldatabrick.dfs.core.windows.net/circuits.csv"))