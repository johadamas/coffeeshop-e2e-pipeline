# Databricks notebook source
# MAGIC %md
# MAGIC ### 1. Get Storage Credentials

# COMMAND ----------

# Retrieve the storage account name and key from the Azure Databricks secrets
storage_account_name = dbutils.secrets.get(scope='adbSecretScope', key='storageAccountName')
storage_account_key = dbutils.secrets.get(scope='adbSecretScope', key='storageAccountKey')

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Container Mounting

# COMMAND ----------

# MAGIC %md
# MAGIC #### A. Function to mount containers

# COMMAND ----------

# Function to Mount Containers
def mount_container(container_name, mount_point, storage_account_name, storage_account_key):
    # Check if the storage is already mounted
    if any(mount.mountPoint == mount_point for mount in dbutils.fs.mounts()):
        print(f"Storage already mounted at {mount_point}")
    else:
        # Mount the container
        dbutils.fs.mount(
            source=f'wasbs://{container_name}@{storage_account_name}.blob.core.windows.net',
            mount_point=mount_point,
            extra_configs={f'fs.azure.account.key.{storage_account_name}.blob.core.windows.net': storage_account_key}
        )
        print(f"Storage mounted at {mount_point}")


# COMMAND ----------

# MAGIC %md
# MAGIC #### B. Define mounting points

# COMMAND ----------

# Mount points and container names
containers = [
    {"container_name": "landing", "mount_point": "/mnt/raw_coffee_shop/"},
    {"container_name": "checkpoints", "mount_point": "/mnt/stream_checkpoints/"},
    {"container_name": "medallion", "mount_point": "/mnt/medallion/"}
]

# COMMAND ----------

# MAGIC %md
# MAGIC #### C. Call and loop through the function

# COMMAND ----------

# Loop through containers and mount each one
for container in containers:
    mount_container(container["container_name"], container["mount_point"], storage_account_name, storage_account_key)
