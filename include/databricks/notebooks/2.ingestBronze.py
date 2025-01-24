# Databricks notebook source
# MAGIC %md
# MAGIC ## Bronze Data Ingestion

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Define the schema

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

# Define the schema for the coffee shop data
schema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("transaction_time", StringType(), True),
    StructField("transaction_qty", IntegerType(), True),
    StructField("store_id", StringType(), True),
    StructField("store_location", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("unit_price", FloatType(), True),
    StructField("product_category", StringType(), True),
    StructField("product_name", StringType(), True),
    StructField("payment_type", StringType(), True),
    StructField("customer_name", StringType(), True)
])

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Define the container path

# COMMAND ----------

# Define the input and output paths
landing_path  = "dbfs:/mnt/raw_coffee_shop/raw_coffee_shop/" 
checkpoints_path = "dbfs:/mnt/stream_checkpoints/"

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Initiate Autoloader

# COMMAND ----------

# Read the streaming data and add 'extract_time' column
from pyspark.sql import functions as sf

stream_df = spark.readStream\
    .format("cloudFiles")\
    .option("cloudFiles.format", "csv")\
    .option("schemaLocation", f"{checkpoints_path}/schema_infer")\
    .option("checkpointLocation", f"{checkpoints_path}/checkpoint")\
    .option("header", "true")\
    .schema(schema)\
    .load(landing_path)\
    .withColumn("extract_time", sf.current_timestamp())

# COMMAND ----------

# Display the streaming data (optional)
# display(stream_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Save the bronze data

# COMMAND ----------

# Write the stream data into the bronze directory
bronze = stream_df.writeStream\
        .format("delta")\
        .option("checkpointLocation", f"{checkpoints_path}/bronze")\
        .option("mergeSchema", "true") \
        .outputMode("append")\
        .queryName("BronzeCoffeeStream")\
        .trigger(availableNow=True)\
        .start("dbfs:/mnt/medallion/bronze/")
