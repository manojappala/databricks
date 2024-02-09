# Databricks notebook source
# MAGIC %md
# MAGIC #Access keys

# COMMAND ----------

spark.conf.set("fs.azure.account.key<storage-account>.dfs.core.windows.net","<access-key>")

# COMMAND ----------

spark.conf.set("fs.azure.account.key.testdljan2024.dfs.core.windows.net","9eM3A0waKH5/tM2cBE6BAcGmI/6kDvZjsP7w4I/4HTivCrwWdxFb0kC8XwUleehaklbNN0P+u60h+AStJHNQuw==")

# COMMAND ----------

dbutils.fs.ls("abfss://raw@testdljan2024.dfs.core.windows.net/ecdc")

# COMMAND ----------

display(dbutils.fs.ls("abfss://raw@testdljan2024.dfs.core.windows.net/ecdc"))

# COMMAND ----------

display(spark.read.csv("abfss://raw@testdljan2024.dfs.core.windows.net/ecdc/country_response.csv"))

# COMMAND ----------


