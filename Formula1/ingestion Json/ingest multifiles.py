# Databricks notebook source
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

# COMMAND ----------

lap_time_schema = StructType(fields=[
                    StructField("raceId", IntegerType(), False),
                    StructField("driverId", IntegerType(), True),
                    StructField("lap", IntegerType(), True),
                    StructField("position", IntegerType(), True),
                    StructField("time", DateType(), True),
                    StructField("milliseconds", IntegerType(), True)
])

# COMMAND ----------

lap_times_df = spark.read.schema(lap_time_schema).csv("/mnt/testdljan2024/raw/formula1/lap_times")

# COMMAND ----------

display(lap_times_df)

# COMMAND ----------

lap_times_df.count()

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

final_df = lap_times_df.withColumnRenamed("driverId", "driver_id") \
                       .withColumnRenamed("raceId", "race_id")\
                       .withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

display(final_df)

# COMMAND ----------

final_df.write.mode("overwrite").parquet("/mnt/testdljan2024/raw/formula1/processed_lap_times")

# COMMAND ----------


