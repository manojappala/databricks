# Databricks notebook source
# MAGIC %md 
# MAGIC #Service principal
# MAGIC 	Ø Register azure AD application /service principle
# MAGIC 	Ø Generate a secret/password for the app
# MAGIC 	Ø Set spark config with app/client id, directory/tenant id & secret
# MAGIC 	Ø Assign Role storage Blob data contributor /reader to data lake
# MAGIC

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.<storage-account>.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.<storage-account>.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.<storage-account>.dfs.core.windows.net", "<application-id>")
spark.conf.set("fs.azure.account.oauth2.client.secret.<storage-account>.dfs.core.windows.net", service_credential)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.<storage-account>.dfs.core.windows.net", "https://login.microsoftonline.com/<directory-id>/oauth2/token")

# COMMAND ----------

client_id = "387090d0-2f8d-4a93-8c5a-f40f54d8b13a"
tenant_id = "3061db22-5555-4afd-8c9f-12d8a05a41b2"
secret_value = "q4S8Q~UB~X17urzQUoGSDOsCu29lCWd7IET_dawN"

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.testdljan2024.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.testdljan2024.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.testdljan2024.dfs.core.windows.net", client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.testdljan2024.dfs.core.windows.net", secret_value)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.testdljan2024.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# COMMAND ----------

dbutils.fs.ls('abfss://<container name>@<storage account name>.dfs.core.windows.net')

# COMMAND ----------

display(dbutils.fs.ls("abfss://raw@testdljan2024.dfs.core.windows.net/"))

# COMMAND ----------

display(spark.read.csv("abfss://raw@testdljan2024.dfs.core.windows.net/ecdc/country_lookup.csv"))

# COMMAND ----------


