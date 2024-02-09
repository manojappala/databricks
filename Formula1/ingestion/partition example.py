# Databricks notebook source
# MAGIC %md
# MAGIC Partition example

# COMMAND ----------

races_selected_df.write.mode('overwrite').partitionBy('race_year').parquet('/mnt/formula1/proessed/races')
