# Databricks notebook source
# MAGIC %md
# MAGIC ingest circuit.csv files
# MAGIC

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

display(dbutils.fs.ls("/mnt/testdljan2024/raw/formula1"))

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/testdljan2024/raw/formula1

# COMMAND ----------

spark.read.csv("dbfs:/mnt/testdljan2024/raw/formula1/circuits.csv")

# COMMAND ----------

circuits_df = spark.read.csv("dbfs:/mnt/testdljan2024/raw/formula1/circuits.csv")

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

type("dbfs:/mnt/testdljan2024/raw/formula1/circuits.csv")

# COMMAND ----------

type(circuits_df)

# COMMAND ----------

circuits_df.show()

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

circuits_df = spark.read.option("header", True).csv("dbfs:/mnt/testdljan2024/raw/formula1/circuits.csv")

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

circuits_df.printSchema()

# COMMAND ----------

circuits_df.describe().show()

# COMMAND ----------

circuits_df = spark.read.option("header", True).option("inferScheme", True).csv("dbfs:/mnt/testdljan2024/raw/formula1/circuits.csv")

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType

# COMMAND ----------

circuits_schema = StructType(fields=[
    StructField("circuitId", IntegerType(), False),
    StructField("circuitRef", StringType(), True),
    StructField("name", StringType(), True),
    StructField("location", StringType(), True),
    StructField("country", StringType(), True),
    StructField("lat", DoubleType(), True),
    StructField("lng", DoubleType(), True),
    StructField("alt", IntegerType(), True),
    StructField("url", IntegerType(), True),
    ])

# COMMAND ----------

circuits_df = spark.read.option("header", True).schema(circuits_schema).csv("dbfs:/mnt/testdljan2024/raw/formula1/circuits.csv")

# COMMAND ----------

circuits_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC select the columns we want

# COMMAND ----------

circuits_selected_df = circuits_df.select("circuitId", "circuitRef", "name", "location", "country", "lat", "lng", "alt")

# COMMAND ----------

display (circuits_selected_df)

# COMMAND ----------

circuits_selected_df = circuits_df.select(circuits_df.circuitId, circuits_df.circuitRef, circuits_df.name, circuits_df.location, circuits_df.location,  circuits_df.lat, circuits_df.lng, circuits_df.alt)

# COMMAND ----------

circuits_selected_df = circuits_df.select(circuits_df["circuitId"], circuits_df["circuitRef"],
                                           circuits_df["name"], circuits_df["location"], circuits_df["country"], circuits_df["lat"], circuits_df["lng"], circuits_df["alt"])

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

circuits_selected_df = circuits_df.select(col("circuitId"), col("circuitRef"), col("name"), col("location"), col("country"), col("lat"), col("lng"), col("alt"))

# COMMAND ----------

display (circuits_selected_df)

# COMMAND ----------

circuits_selected_df = circuits_df.select(col("circuitId"), col("circuitRef"), col("name"), col("location"), col("country").alias("race_country"), col("lat"), col("lng"), col("alt"))

# COMMAND ----------

display (circuits_selected_df)

# COMMAND ----------

circuits_selected_df = circuits_df.select(col("circuitId"), col("circuitRef"), col("name"), col("location"), col("country").alias("race_country"), col("lat").alias("latitude"), col("lng").alias("longitude"), col("alt").alias("altitude"))

# COMMAND ----------

display (circuits_selected_df)

# COMMAND ----------

# MAGIC %md
# MAGIC we can aslo change the colums name with data.frame.withcolumnrenames(existing,new)

# COMMAND ----------

circuits_renamed_df = circuits_selected_df.withColumnRenamed("circuitId" , "circuit_id") \
    .withColumnRenamed("circuitRef", "circuit_ref") \
    .withColumnRenamed("lat", "latitude") \
    .withColumnRenamed("lng", "longitude") \
    .withColumnRenamed("alt", "altitude") 

# COMMAND ----------

display(circuits_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Add new column
# MAGIC #####here in this example we will add new column with current timestamp

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

circuits_final_df = circuits_renamed_df.withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

display(circuits_final_df)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

circuits_final_df = circuits_renamed_df.withColumn("ingestion_date", current_timestamp()) \
    .withColumn("env", lit("Production"))


# COMMAND ----------

display(circuits_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Dataframe write

# COMMAND ----------

circuits_final_df.write.parquet("/mnt/testdljan2024/raw/formula1/processed/circuits")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/testdljan2024/raw/formula1/processed/circuits

# COMMAND ----------

df = spark.read.parquet("/mnt/testdljan2024/raw/formula1/processed/circuits")

# COMMAND ----------

display(df)

# COMMAND ----------

circuits_final_df.write.mode("overwrite").parquet("/mnt/testdljan2024/raw/formula1/processed/circuits")

# COMMAND ----------


