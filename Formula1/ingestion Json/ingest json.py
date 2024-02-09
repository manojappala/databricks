# Databricks notebook source
# MAGIC %md
# MAGIC How to read json file

# COMMAND ----------

constructors_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, URL STRING"

# COMMAND ----------

constructor_df = spark.read.schema(constructors_schema).json('/mnt/testdljan2024/raw/formula1/constructors.json')

# COMMAND ----------

constructor_df.printSchema()

# COMMAND ----------

display(constructor_df)

# COMMAND ----------

# MAGIC %md
# MAGIC drop unwanted columns from dataframe

# COMMAND ----------

constructor_drop_df = constructor_df.drop('url')

# COMMAND ----------

constructor_drop_df = constructor_df.drop(constructor_df['url'])

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

constructor_drop_df = constructor_df.drop(col('url'))

# COMMAND ----------

display(constructor_drop_df)

# COMMAND ----------

# MAGIC %md rename colmns and ingestion date

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

constructor_final_df = constructor_drop_df.withColumnRenamed("constructorId", "constructor_id").withColumnRenamed("constructorRef", "constructor_ref").withColumn("ingestiondate", current_timestamp())

# COMMAND ----------

display(constructor_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC write output to parquet file

# COMMAND ----------

constructor_final_df.write.mode("overwrite").parquet("/mnt/testdljan2024/raw/formula1/constructors")

# COMMAND ----------


