# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Azure Data Lake Using service principle
# MAGIC 1.Register Azure AD application/service principle
# MAGIC
# MAGIC 2.Generate secret/password for the application
# MAGIC
# MAGIC 3.spark config with app//client id,tenet id and secret
# MAGIC
# MAGIC 4.Assign role  storage blob data contributor to the data lake

# COMMAND ----------

client_id = dbutils.secrets.get(scope = 'formula1_scope',key = 'formula1-app-client-id')
tenant_id = dbutils.secrets.get(scope = 'formula1_scope',key = 'formula1-app-tenant-id')
client_secret = dbutils.secrets.get(scope = 'formula1_scope',key = 'formula1-app-client-secret')

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.formula1dlldatabrick.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.formula1dlldatabrick.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.formula1dlldatabrick.dfs.core.windows.net", client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.formula1dlldatabrick.dfs.core.windows.net", client_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.formula1dlldatabrick.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1dlldatabrick.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1dlldatabrick.dfs.core.windows.net/circuits.csv"))