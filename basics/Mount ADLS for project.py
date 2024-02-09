# Databricks notebook source
# MAGIC %md
# MAGIC #Mount ADLS

# COMMAND ----------

def mount_adls(storage_name, Container_name):
    client_id = "9e7e71c2-9936-4ab8-bd3c-b074de08e612"
    tenant_id = "3061db22-5555-4afd-8c9f-12d8a05a41b2"
    client_secret = "ZqZ8Q~Sxr5TIbCrxt3tW0NBuS.NWYLn5aqgCWcy3"

    configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}
    
    dbutils.fs.mount(
        source = f"abfss://{Container_name}@{storage_name}.dfs.core.windows.net/",
        mount_point = f"/mnt/{storage_name}/{Container_name}",
        extra_configs = configs)
    
    display(dbutils.fs.mounts())

# COMMAND ----------

mount_adls('testdljan2024','raw')

# COMMAND ----------

display(dbutils.fs.ls("/mnt/testdljan2024/raw"))

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

dbutils.fs.unmount("/mnt/testdljan2024/raw")

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------


