# Databricks notebook source
# MAGIC %md
# MAGIC Nested Json file

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

# COMMAND ----------

name_schema = StructType(fields=[StructField("forename", StringType(), True), 
                                 StructField("surname", StringType(), True)
])

# COMMAND ----------

driver_schema= StructType(fields=[
    StructField("driverId", IntegerType(), False),
    StructField("driverRef", StringType(), True),
    StructField("number", IntegerType(), True),
    StructField("code", StringType(), True),
    StructField("name", name_schema),
    StructField("dob", DateType(), True),
    StructField("nationality", StringType(), True),
    StructField("url", StringType(), True),
])

# COMMAND ----------

driver_df = spark.read.schema(driver_schema).json("/mnt/testdljan2024/raw/formula1/drivers.json")

# COMMAND ----------

driver_df.printSchema()

# COMMAND ----------

display(driver_df)

# COMMAND ----------

from pyspark.sql.functions import col, concat, current_timestamp, lit

# COMMAND ----------

driver_with_columns_df = driver_df.withColumnRenamed("driverId", "driver_id")\
                                  .withColumnRenamed("driverRef", "driver_ref")\
                                  .withColumn("ingestion_date", current_timestamp()) \
                                  .withColumn("name", concat(col("name.forename"), lit (" "), col("name.surname")))

# COMMAND ----------

display(driver_with_columns_df)

# COMMAND ----------

driver_final_df = driver_with_columns_df.drop(col("url"))

# COMMAND ----------

display (driver_final_df)

# COMMAND ----------

driver_final_df.write.mode("overwrite").parquet("/mnt/testdljan2024/raw/formula1/drivers")

# COMMAND ----------


