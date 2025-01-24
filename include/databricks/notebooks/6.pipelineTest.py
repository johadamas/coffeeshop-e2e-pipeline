# Databricks notebook source
# MAGIC %md
# MAGIC ### Pipeline Result Check

# COMMAND ----------

# Example query to select all columns from the fact_flights table
fact_transactions_df = spark.sql("SELECT * FROM gold.fact_transactions")

# COMMAND ----------

# Display the query result
# display(fact_transactions_df)

# COMMAND ----------

# Count distinct transaction_id values
distinct_transaction_count = fact_transactions_df.select("transaction_id").distinct().count()

# Display the result
print(f"Number of distinct transaction_id: {distinct_transaction_count}")

# COMMAND ----------

def count_rows_in_tables(tables):
    counts = {}
    for table in tables:
        table_df = spark.sql(f"SELECT * FROM gold.{table}")
        row_count = table_df.count()
        counts[table] = row_count
        print(f"Table: gold.{table}, Row Count: {row_count}")
    return counts

# List of gold tables
gold_tables = ["dim_store", "dim_product", "dim_customer", "dim_payment", "fact_transactions"]

# Call the function to count rows in each table
table_counts = count_rows_in_tables(gold_tables)

# COMMAND ----------

# Select distinct extract_time, transformed_time, and load_time
distinct_times_df = spark.sql("""
    SELECT DISTINCT load_time 
    FROM gold.fact_transactions
""")

# Display the distinct times
display(distinct_times_df)
