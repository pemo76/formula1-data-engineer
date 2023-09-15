# Databricks notebook source
# MAGIC %md
# MAGIC ####Explore DBFS root
# MAGIC 1.List all the folder in dbfs root
# MAGIC
# MAGIC 2.Interect with dbfs file browser
# MAGIC
# MAGIC 3.Upload file to dbfs root

# COMMAND ----------

display(dbutils.fs.ls('/'))

# COMMAND ----------

display(dbutils.fs.ls('/FileStore/tables'))