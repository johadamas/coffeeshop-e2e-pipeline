# Databricks notebook source
# MAGIC %md
# MAGIC ## Transform Bronze to Silver

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Define the container path

# COMMAND ----------

# Define the paths
bronze_path = "dbfs:/mnt/medallion/bronze/"
silver_path = "dbfs:/mnt/medallion/silver/"
checkpoints_path = "dbfs:/mnt/stream_checkpoints/"

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Initiate Autoloader

# COMMAND ----------

# Read the streaming data from the bronze directory
bronze_df = spark.readStream\
        .format("delta")\
        .load(bronze_path)

# COMMAND ----------

# Display the streaming data (optional)
# display(bronze_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Silver Transformation

# COMMAND ----------

# MAGIC %md
# MAGIC #### A. Standarized Column Names

# COMMAND ----------

def correct_naming_conventions(df):
    # Function to standardize column names
    def standardize_column_name(col_name):
        return col_name.lower().replace(" ", "_")

    # Apply the standardization function to all column names
    new_column_names = [standardize_column_name(col_name) for col_name in df.columns]

    # Rename the columns in the DataFrame
    df = df.toDF(*new_column_names)
    return df

# COMMAND ----------

standardized_df = correct_naming_conventions(bronze_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### B. Handling Null and Duplicate

# COMMAND ----------

# Handling Null and Duplicate Function
def handling_nulls_and_duplicates(df):
    print("Handling nulls and duplicates: ", end="")

    # Drop duplicates
    df_no_dup = df.dropDuplicates()

    # Separate string and numeric columns
    string_columns = [col for col, dtype in df.dtypes if dtype == "string"]
    numeric_columns = [col for col, dtype in df.dtypes if dtype in ["int", "double", "float"]]

    # Fill nulls for string columns with 'Unknown'
    df_string = df_no_dup.fillna("Unknown", subset=string_columns)

    # Fill nulls for numeric columns with 0
    df_clean = df_string.fillna(0, subset=numeric_columns)

    return df_clean

# COMMAND ----------

cleaned_df = handling_nulls_and_duplicates(standardized_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### C. Add 'daypart' column

# COMMAND ----------

from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType
from datetime import datetime

# Define the UDF to classify the daypart
def classify_daypart(transaction_time):
    time = datetime.strptime(transaction_time, '%H:%M:%S').time()
    if time >= datetime.strptime('06:00:00', '%H:%M:%S').time() and time < datetime.strptime('12:00:00', '%H:%M:%S').time():
        return 'Morning'
    elif time >= datetime.strptime('12:00:00', '%H:%M:%S').time() and time < datetime.strptime('18:00:00', '%H:%M:%S').time():
        return 'Afternoon'
    elif time >= datetime.strptime('18:00:00', '%H:%M:%S').time() and time < datetime.strptime('22:00:00', '%H:%M:%S').time():
        return 'Evening'
    else:
        return 'Night'

# Register the UDF
classify_daypart_udf = udf(classify_daypart, StringType())

# COMMAND ----------

# Apply the UDF to create the 'daypart' column
df_with_daypart = cleaned_df.withColumn('daypart', classify_daypart_udf(col('transaction_time')))

# COMMAND ----------

# Display the resulting DataFrame
# display(df_with_daypart)

# COMMAND ----------

# MAGIC %md
# MAGIC #### D. Add 'transform_time' column

# COMMAND ----------

# Create 'transform_time' column
from pyspark.sql import functions as sf

df_silver = df_with_daypart.withColumn('transformed_time', sf.current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Save the silver data

# COMMAND ----------

# Write the transformed data into silver
silver = df_silver.writeStream \
    .format("delta") \
    .option("checkpointLocation", f"{checkpoints_path}/silver") \
    .option("mergeSchema", "true") \
    .outputMode("append") \
    .queryName("SilverCoffeeStream") \
    .trigger(availableNow=True) \
    .start(silver_path)
