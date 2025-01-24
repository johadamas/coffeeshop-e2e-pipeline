# Databricks notebook source
# MAGIC %md
# MAGIC ## Load into Delta Tables

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Create gold database

# COMMAND ----------

# Create gold database if it doesn't exist
spark.sql("CREATE DATABASE IF NOT EXISTS gold")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Define the gold dir path

# COMMAND ----------

# Define the base path to the gold directory
gold_path = "dbfs:/mnt/medallion/gold"

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Load to delta functions

# COMMAND ----------

# Functions to load all the gold data into delta tables
def create_gold_tables(gold_base_path):
    tables = ["dim_store", "dim_product", "dim_customer", "dim_payment", "fact_transactions"]
    for table in tables:
        table_path = f"{gold_base_path}/{table}"
        spark.sql(f"CREATE TABLE IF NOT EXISTS gold.{table} USING DELTA LOCATION '{table_path}'")
        print(f"Table gold.{table} created at location {table_path}.")

# COMMAND ----------

# Call the function to create the Delta tables for the gold tables
create_gold_tables(gold_path)
