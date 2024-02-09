# Databricks notebook source
# MAGIC %md
# MAGIC #Mount ADLS

# COMMAND ----------

client_id = "9e7e71c2-9936-4ab8-bd3c-b074de08e612"
tenant_id = "3061db22-5555-4afd-8c9f-12d8a05a41b2"
client_secret = "ZqZ8Q~Sxr5TIbCrxt3tW0NBuS.NWYLn5aqgCWcy3"

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.testdljan2024.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.testdljan2024.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.testdljan2024.dfs.core.windows.net", client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.testdljan2024.dfs.core.windows.net", client_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.testdljan2024.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# COMMAND ----------

dbutils.fs.ls("abfss://demo@testdljan2024.dfs.core.windows.net/")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@testdljan2024.dfs.core.windows.net/"))

# COMMAND ----------


