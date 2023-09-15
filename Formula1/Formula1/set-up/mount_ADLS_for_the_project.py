# Databricks notebook source
# MAGIC %md
# MAGIC #### Mount Azure Data Lake Container for the Project
# MAGIC 1. Get client id,tenant id from key vault
# MAGIC
# MAGIC 2.set spark config with client id, tenant id and secret
# MAGIC
# MAGIC 3.call file system utility mount to the storage
# MAGIC
# MAGIC 4.Explore other file system utilites

# COMMAND ----------

def mount_adls(storage_account_name, container_name):
    #get secret from key vault
    client_id = dbutils.secrets.get(scope = 'formula1_scope',key = 'formula1-app-client-id')
    tenant_id = dbutils.secrets.get(scope = 'formula1_scope',key = 'formula1-app-tenant-id')
    client_secret = dbutils.secrets.get(scope = 'formula1_scope',key = 'formula1-app-client-secret')

    #set spark config
    configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}
     
    # Unmount the mount point if it already exists
    if any(mount.mountPoint == f"/mnt/{storage_account_name}/{container_name}" for mount in dbutils.fs.mounts()):
        dbutils.fs.unmount(f"/mnt/{storage_account_name}/{container_name}")


    #mount the storage account container
    dbutils.fs.mount(
      source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
      mount_point = f"/mnt/{storage_account_name}/{container_name}",
      extra_configs = configs)
    
    display(dbutils.fs.mounts())
    

# COMMAND ----------

# MAGIC %md
# MAGIC ###Mount Raw container

# COMMAND ----------

mount_adls('formula1dlldatabrick','raw')

# COMMAND ----------

# MAGIC %md
# MAGIC ##Mount Presentation container

# COMMAND ----------

mount_adls('formula1dlldatabrick','presentation')

# COMMAND ----------

# MAGIC %md
# MAGIC ###Mount Processed container

# COMMAND ----------

mount_adls('formula1dlldatabrick','processed')