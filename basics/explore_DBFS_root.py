# Databricks notebook source
# MAGIC %md
# MAGIC #Explore DBFS root

# COMMAND ----------

dbutils.fs.ls('/')

# COMMAND ----------

display(dbutils.fs.ls('/'))

# COMMAND ----------

display(dbutils.fs.ls('/FileStore'))

# COMMAND ----------

display(spark.read.csv('/FileStore/dim_date.csv'))

# COMMAND ----------


