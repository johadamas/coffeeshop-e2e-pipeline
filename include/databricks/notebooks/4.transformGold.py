# Databricks notebook source
# MAGIC %md
# MAGIC ## Transform Silver to Gold

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Define the container path

# COMMAND ----------

# Define the paths
silver_path = "dbfs:/mnt/medallion/silver/"
gold_path = "dbfs:/mnt/medallion/gold/"
checkpoints_path = "dbfs:/mnt/stream_checkpoints/"

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Read the Silver Data

# COMMAND ----------

# Read the streaming data from the silver directory
silver_df = spark.read\
        .format("delta")\
        .load(silver_path)

# COMMAND ----------

# Display the silver data (optional)
# display(silver_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Gold Transformation

# COMMAND ----------

# MAGIC %md
# MAGIC #### A. dim_store

# COMMAND ----------

dim_store = silver_df.select("store_id", "store_location").distinct()

# COMMAND ----------

dim_store.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC #### B. dim_product

# COMMAND ----------

dim_product = silver_df.select("product_id", "product_name", "unit_price", "product_category").distinct()

# COMMAND ----------

dim_product.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC #### C. dim_customer

# COMMAND ----------

from pyspark.sql import functions as sf

dim_customer = silver_df.select("customer_name").distinct() \
    .withColumn("customer_id", sf.monotonically_increasing_id().cast("string"))

# COMMAND ----------

dim_customer.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC #### D. dim_payment

# COMMAND ----------

dim_payment = silver_df.select("payment_type").distinct() \
    .withColumn("payment_id", sf.monotonically_increasing_id().cast("string"))

# COMMAND ----------

dim_payment.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC #### E. fact_transactions

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp

# Create Fact Table by Joining with Dimension Tables
fact_transactions = silver_df \
    .join(dim_customer, "customer_name") \
    .join(dim_payment, "payment_type") \
    .select("transaction_id", "transaction_time", "daypart",  "transaction_qty", "store_id", "product_id", "unit_price", "customer_id", "payment_id", "extract_time", "transformed_time")\
    .withColumn('load_time', sf.current_timestamp())

# COMMAND ----------

fact_transactions.printSchema()

# COMMAND ----------

# display(fact_transactions)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Save the gold data

# COMMAND ----------

def write_to_gold(dim_store, dim_product, dim_customer, dim_payment, fact_transactions, gold_path):

    dim_store.write.format("delta").mode("overwrite").save(f"{gold_path}/dim_store")
    dim_product.write.format("delta").mode("overwrite").save(f"{gold_path}/dim_product")
    dim_customer.write.format("delta").mode("overwrite").save(f"{gold_path}/dim_customer")
    dim_payment.write.format("delta").mode("overwrite").save(f"{gold_path}/dim_payment")
    fact_transactions.write.format("delta").mode("append").option("mergeSchema", "true").save(f"{gold_path}/fact_transactions")

    print("DataFrames successfully written to the gold directory.")

# COMMAND ----------

# Define the base path to the gold directory
gold_path = "dbfs:/mnt/medallion/gold/"

# Call the function to write the DataFrames to the gold directory
write_to_gold(dim_store, dim_product, dim_customer, dim_payment, fact_transactions, gold_path)
