# Databricks notebook source
dbutils.notebook.help()

# COMMAND ----------

v_result = dbutils.notebook.run("ingest circuit file_widgets", 0 , {"p_data_source": "Ergast API"})

# COMMAND ----------

v_result
