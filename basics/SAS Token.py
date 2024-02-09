# Databricks notebook source
# MAGIC %md
# MAGIC #SAS Tocken Access

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.<storage-account>.dfs.core.windows.net","SAS")
spark.conf.set("fs.azure.sas.token.provider.type.<storage-account>.dfs.core.windows.net","org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.<storage-account>.dfs.core.windows.net","<Token>")

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.testdljan2024.dfs.core.windows.net","SAS")
spark.conf.set("fs.azure.sas.token.provider.type.testdljan2024.dfs.core.windows.net","org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.testdljan2024.dfs.core.windows.net","sp=rl&st=2024-02-07T03:56:44Z&se=2024-02-07T11:56:44Z&spr=https&sv=2022-11-02&sr=c&sig=odabjvJw3gu7tX4KvD%2FsYYyoVh3KK3wKJp0F3ZA3XGQ%3D")

# COMMAND ----------

dbutils.fs.ls("abfss://demo@testdljan2024.dfs.core.windows.net/")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@testdljan2024.dfs.core.windows.net/"))

# COMMAND ----------


