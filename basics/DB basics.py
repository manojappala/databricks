# Databricks notebook source
# MAGIC %md
# MAGIC #intorduction to DB
# MAGIC ##learning Databricks

# COMMAND ----------

# MAGIC %sql
# MAGIC select "Hello"

# COMMAND ----------

message = " welcome to DB"

# COMMAND ----------

print(message)

# COMMAND ----------

# MAGIC %fs
# MAGIC ls

# COMMAND ----------

# MAGIC %fs 
# MAGIC ls databricks-datasets/COVID/

# COMMAND ----------

dbutils.fs.help()

# COMMAND ----------

dbutils.fs.help('ls')

# COMMAND ----------


