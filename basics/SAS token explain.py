# Databricks notebook source
# MAGIC %md 
# MAGIC #SAS Token

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.<storage-account>.dfs.core.windows.net","SAS")
spark.conf.set("fs.azure.sas.token.provider.type.<storage-account>.dfs.core.windows.net","org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.<storage-account>.dfs.core.windows.net","<Token>")

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.testdljan2024.dfs.core.windows.net","SAS")
spark.conf.set("fs.azure.sas.token.provider.type.testdljan2024.dfs.core.windows.net","org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.testdljan2024.dfs.core.windows.net","sp=rl&st=2024-02-07T22:06:57Z&se=2024-02-08T06:06:57Z&spr=https&sv=2022-11-02&sr=c&sig=II5P%2BdMngBSdwvAsSdMmhmhFXDUw0TrG7lraxnbyPSY%3D")

# COMMAND ----------

dbutils.fs.ls('abfss://<container name>@<storage account name>.dfs.core.windows.net')

# COMMAND ----------

display(dbutils.fs.ls("abfss://raw@testdljan2024.dfs.core.windows.net/ecdc"))

# COMMAND ----------

display(spark.read.csv("abfss://raw@testdljan2024.dfs.core.windows.net/ecdc/country_lookup.csv"))

# COMMAND ----------


