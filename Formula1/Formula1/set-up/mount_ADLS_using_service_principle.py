# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Azure Data Lake Using service principle
# MAGIC 1. Get client id,tenant id from key vault
# MAGIC
# MAGIC 2.set spark config with client id, tenant id and secret
# MAGIC
# MAGIC 3.call file system utility mount to the storage
# MAGIC
# MAGIC 4.Explore other file system utilites

# COMMAND ----------

client_id = dbutils.secrets.get(scope = 'formula1_scope',key = 'formula1-app-client-id')
tenant_id = dbutils.secrets.get(scope = 'formula1_scope',key = 'formula1-app-tenant-id')
client_secret = dbutils.secrets.get(scope = 'formula1_scope',key = 'formula1-app-client-secret')

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# COMMAND ----------

dbutils.fs.mount(
  source = "abfss://demo@formula1dlldatabrick.dfs.core.windows.net/",
  mount_point = "/mnt/formula1dlldatabrick/demo",
  extra_configs = configs)

# COMMAND ----------

display(dbutils.fs.ls("/mnt/formula1dlldatabrick/demo"))

# COMMAND ----------

display(spark.read.csv("/mnt/formula1dlldatabrick/demo/circuits.csv"))

# COMMAND ----------

display(dbutils.fs.mounts())