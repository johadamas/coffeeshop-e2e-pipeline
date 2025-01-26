# Databricks ETL Pipeline with Autoloader

## Project Overview
This project demonstrates an end-to-end ETL (Extract, Transform, Load) pipeline using Databricks, seamlessly integrating multiple Azure services and modern data engineering tools.

## Architecture

![Pipeline Flow](/images/_project_architecture.png "Project Architecture")

## Key Features
The pipeline is designed to process, transform, and analyze fake Coffee Shop Sales data using the following key features and technologies:

1. **Airflow Orchestration**: 
    - Airflow manages the workflow with two DAGs: one for setting up the infrastructure (using Terraform) and another for generating and loading batch data into Azure Data Lake Storage (ADLS)

1. **Infrastructure as Code**:
    - Terraform provisions Azure resources, including a Storage Account, Databricks Workspace, and Key Vault, ensuring reproducible and scalable infrastructure deployment

1. **Databricks Auto Loader**:
    - Efficiently ingests data incrementally from the Landing Container into the Bronze layer using a schema-on-read approach with checkpointing

1. **The Medallion Architecture**:
    - Bronze Layer: Stores raw ingested data
    - Silver Layer: Cleans and enriches the data, addressing nulls and duplicates while adding meaningful insights
    - Gold Layer: Organizes data into a star schema with fact and dimension tables for analytical consumption

1. **Power BI Integration**:
    - Gold-layer Delta tables are visualized in Power BI using a JDBC/ODBC endpoint, enabling real-time and interactive dashboards



## Get Started

### Prerequisites
- Azure Subscription
- Astro CLI v1.30.0
- Docker Desktop
- Power BI Desktop

### Project Flows:

1. **Run Airflow DAGs**:  
   The Airflow contains two DAGs:
    - `project_setup_pipeline`: Runs terraform task, generates fake coffee shop sales data and then upload the data to the landing container in ADLS

    - `generate_and_load_data`: Generates additional fake coffee shop sales data and uploads it to the landing container automatically. This simulates incoming data in a real-life batch processing scenario

        ![](/images/0.airflow_dags.png "")

2. **Run Airflow DAGs**:  
   The Airflow contains two DAGs:
    - `project_setup_pipeline`: Runs terraform task, generates fake coffee shop sales data and then upload the data to the landing container in ADLS

        ![](/images/1.project_setup_dag.png "")

3. **Verify Azure Resource Group**:  
   - Go to the Azure Resource Group to confirm the resources are successfully created (storage account, databricks workspace, key vault)

        ![](/images/2.resource_group.png "")

4. **Verify Azure Storage Account**:  
    - Ensure the required storage containers (landing, medallion, and checkpoints) are created successfully

        ![](/images/3.storage_containers.png "")

5. **Verify Landing Container**:  
    - Open the landing container to confirm that the fake Coffee Shop Sales data has been uploaded correctly. It should looks like this: `raw_coffee_shop/coffee_shop_data_{timestamp}.csv`
    
        ![](/images/4.landing_container.png "")

6. **Verify Medallion Container**:  
    - Ensure the required medallion directory (bronze, silver, and gold) are created successfully
    
        ![](/images/5.medallion_container.png "")

7. **Secure Storage Access Key with Key Vault**:  
    - Azure Key Vault is used to securely store the Storage Account `key` and `name`

    - These secrets are later used to dynamically connect Databricks to ADLS by combining them with Databricks SecretScope.
    
        ![](/images/6.keyvault_secret.png "")

8. **Verify Databricks Notebooks**:  
   Ensure that all the Databricks notebooks are present in the workspace:
    - `1.storageMount.py`: Mounts the storage account to the Databricks workspace

    - `2.ingestBronze.py`: Initializes Auto Loader to ingest data from the landing container into the bronze directory of the medallion container

    - `3.transformSilver.py`: Transforms bronze data into silver, handling nulls and duplicates while adding a daypart column for additional insights

    - `4.transformGold.py`: Transforms silver data into gold, creating a star schema with tables like fact_transactions, dim_product, dim_store, etc

    - `5.loadDelta.py`: Creates a gold database in the Databricks catalog and loads the gold data into Delta tables

    - `6.pipelineTest.py`: A notebook created to verify that the Databricks job pipeline is functioning as expected 

        ![](/images/7.databricks_notebooks.png "")

9. **Databricks Compute Cluster**:  
    - The compute cluster is configured with minimum resources to achieve the lowest possible DBU/hour, making the project cost-efficient 
    
        ![](/images/8.databricks_cluster.png "")

10. **Databricks Workflows**:  
    - For demonstration purposes, the Databricks workflow runs on an `all-purpose` cluster instead of a `job cluster` for simplicity

    - The pipeline follows the notebook execution flow:
        ```plaintext
        mount_storage >> ingest_bronze >> transform_silver >> transform_gold >> load_delta_tables
        ```
    
        ![](/images/9.databricks_job_1.png "")

11. **ETL Pipeline Run**:  
    - The ETL pipeline successfully runs end-to-end, as demonstrated in the screenshot below

        ![](/images/11.pipeline_runs.png "")

12. **ETL Pipeline Validation**:

    - Using the `pipelineTest` notebook, the pipeline's accuracy was validated by checking the number of distinct transactions. The result confirms exactly `1000` transactions, matching the input parameter defined in the data generation function within the Airflow DAG:

        ```python
        def generate_coffee_shop_data(num_records=1000)
        ```

        ![](/images/12.pipeline_result_1_copy.png "")

    - The added `extract_time`, `transformed_time`, and `load_time` columns were used to validate the timestamps for each stage of the pipeline. This ensures the data was successfully extracted, transformed into the medallion container, and loaded into the Databricks Delta Tables:

        ![](/images/13.pipeline_result_2.png "")

13. **Databricks Delta Tables**:  
    - The gold data which loaded into the Databricks delta table provide features like ACID transactions, Time travel and Scalable metadata handling

        ![](/images/19.adb_delta_tables.gif "")

14. **Checkpoints Container**:  
    - The Checkpoints Container ensures data consistency and prevents duplication by storing metadata about files already ingested by Databricks Auto Loader. This setup allows the pipeline to resume seamlessly and track schema changes, ensuring efficient and reliable ingestion

        ![](/images/14.checkpoint_container.png "")

        ```python
        .option("checkpointLocation", f"{checkpoints_path}/checkpoint")
        .option("schemaLocation", f"{checkpoints_path}/schema_infer")
        ```

        *Note:
        In the ingestBronze.py notebook, the checkpointLocation option is configured for both schema inference and data streaming.*

15. **Loading to Power BI**:  
    - Using Power BI, the gold Delta Tables from Databricks Catalog can be imported seamlessly by utilizing the JDBC/ODBC endpoint connection

        ![](/images/15.pbi_import.png "")

    - After importing the gold data from Databricks, Power BI automatically detects the table relationships, which can be verified or adjusted if needed.

        ![](/images/16.pbi_schema.png "")

16. **Power BI Dashboard**:  
    - The Power BI dashboard below includes interactive features such as `tooltips` and `bookmarks` for seamless filter navigation

        ![](/images/20.pbi_dashboard.gif "")
  
        *Note:
        The dashboards interactive features is added after the final pipeline test below.*

17. **Databricks ETL Pipeline test**:  
   Generating New Data:
    - Using the `generate_and_load_new_data` DAG, new fake coffee shop sales data is generated to simulate incoming data in a batch processing scenario

        ![](/images/17.generate_new_data.gif "")

    Testing the Databricks Pipeline:
    - After the new data is successfully loaded into the `landing_container` and the ETL pipeline is triggered manually, the `pipelineTest` notebook can be used to verify the updated data. 

        ![](/images/18.adb_pipeline_test.gif "")

        *Note:
        The increase in the number of distinct `transaction_id` values from `1000` to `2000` confirms that the Auto Loader is functioning as expected*

## Future Enhancements

1. **Implement Monitoring and Alerting**:  
    - Integrate monitoring tools and automated alerting mechanisms to track pipeline performance, detect anomalies, and ensure timely issue resolution

2. **Integrate Unity Catalog and Delta Live Tables**: 
    - Leverage Unity Catalog for centralized governance, fine-grained data access control, and collaboration. Use Delta Live Tables for declarative ETL pipelines, improving pipeline automation and data quality management.

3. **Strengthen Data Quality Checks**:
    - Introduce more comprehensive data quality validation, including null checks, schema enforcement, and threshold-based anomaly detection, to ensure clean and accurate data at all stages

4. **Enhance Support for Change Data Capture (CDC)**: 
    - Upgrade the ETL pipeline to handle incremental changes from source systems using CDC techniques, ensuring efficient updates and near real-time data synchronization

---

Feel free to explore the code and customize it as needed. Contributions and feedback are always welcome!  
           