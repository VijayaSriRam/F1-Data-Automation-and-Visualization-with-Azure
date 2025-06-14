# Real-World Formula 1 Data Engineering Project

## Overview
In this project, I built a modern, end-to-end data engineering platform on Microsoft Azure. I engineered the architecture to evolve from a traditional data lake into a fully governed **Lakehouse**, capable of handling complex data processing requirements.

The platform ingests historical Formula 1 data from various sources, processes it through a multi-layered architecture using **Azure Databricks** and **Delta Lake**, and orchestrates the entire workflow with **Azure Data Factory**. The final, curated data is made available for analysis and reporting through **Databricks Dashboards** and **Power BI**, with governance and security managed centrally by **Unity Catalog**.

My primary analytical goal was to process historical race data to identify and visualize the most dominant Formula 1 drivers and teams of all time.

## Technologies Used
*   **Cloud Platform:** Microsoft Azure
*   **Data Storage:** Azure Data Lake Storage (ADLS) Gen2
*   **Data Processing:** Azure Databricks (Apache Spark, PySpark, Spark SQL)
*   **Data Lakehouse:** Delta Lake
*   **Data Governance:** Unity Catalog
*   **Orchestration:** Azure Data Factory (ADF)
*   **Security:** Azure Key Vault, Managed Identities, Databricks Secret Scopes
*   **BI & Visualization:** Power BI, Databricks Dashboards

## Project Architecture
I designed and implemented a multi-layered Lakehouse architecture, which is a standard industry pattern for building scalable data platforms.

![Project Architecture Diagram](https://github.com/VijayaSriRam/)

1.  **Raw Layer (ADLS Gen2):** Source data is landed in its original format (CSV, single/multi-line JSON, etc.).
2.  **Bronze Layer (Delta Lake):** Data is read, cleaned, schema-enforced, and enriched with audit columns. It is then stored as Delta tables.
3.  **Silver & Gold Layers (Delta Lake):** Data from the Bronze layer is joined and aggregated to create business-ready, analytical tables (e.g., `race_results`, `driver_standings`).
4.  **Governance (Unity Catalog):** A centralized metastore provides unified access control, data lineage, and auditing across the platform.
5.  **Orchestration (Azure Data Factory):** ADF pipelines automate the execution of the entire workflow, from ingestion to transformation, on a schedule.
6.  **Consumption (Power BI & Databricks):** The final presentation tables are connected to Power BI for interactive BI reporting and are also used to build dashboards directly within Databricks.

## Dataset
This project is built using historical Formula 1 data originally sourced from the Ergast Developer API. The dataset includes:
*   `circuits.csv`
*   `races.csv`
*   `constructors.json`
*   `drivers.json`
*   `results.json`
*   `pit_stops.json`
*   `lap_times/` (folder of CSV files)
*   `qualifying/` (folder of multi-line JSON files)

## How to Replicate This Project

To set up and run this project, you can follow these steps. The notebooks in this repository are numbered to guide you through the process.

**1. Azure Environment Setup**
*   First, you will need an active Azure subscription.
*   Provision the following Azure resources:
    *   **Azure Databricks Workspace** (Premium tier is required for Unity Catalog).
    *   **Azure Data Lake Storage Gen2 Account**.
    *   **Azure Key Vault** for secrets management.
    *   **Azure Data Factory** for orchestration.
    *   **Access Connector for Azure Databricks** (this is required for Unity Catalog to access your storage).
*   Configure the necessary permissions, such as granting the Access Connector the `Storage Blob Data Contributor` role on your storage account.

**2. Databricks Workspace Configuration**
*   Import the notebooks from this repository into your Azure Databricks workspace.
*   Create a Databricks cluster using a Unity Catalog-compatible runtime (e.g., 11.3 LTS or higher) and access mode (Single User or Shared).
*   Run the initial setup notebooks to:
    *   Store your ADLS Gen2 Access Key in your Azure Key Vault.
    *   Create a Databricks Secret Scope backed by your Key Vault.
    *   Configure Unity Catalog by creating a Metastore, Catalogs (`f1_processed`, `f1_presentation`), Storage Credentials, and External Locations.

**3. Initial Data Load**
*   Upload the provided raw data files into the `raw` container of your ADLS Gen2 account.

**4. Running the Data Pipeline Notebooks**
*   Execute the notebooks sequentially, following the numbered prefixes which indicate the correct order.
    1.  **Ingestion (Bronze Layer):** Run the ingestion notebooks (e.g., `1_ingest_circuits_file.py`). These will read the raw files, apply transformations, and create the initial Delta tables in your `f1_processed` catalog.
    2.  **Transformation (Silver/Gold Layers):** Run the transformation notebooks to create the final aggregated tables like `race_results`, `driver_standings`, and `calculated_race_results` in your `f1_presentation` catalog.
    3.  **Analysis:** Execute the analysis notebooks to query the presentation tables and generate insights on dominant drivers and teams.

**5. Automating with Azure Data Factory**
*   In your Azure Data Factory, create a **Linked Service** to securely connect to your Databricks workspace. Use a **Managed Identity** for authentication.
*   Recreate the ADF pipelines from the project to orchestrate the execution of the Databricks notebooks. The key steps are:
    *   Build pipelines for ingestion and transformation tasks.
    *   Create a master pipeline that runs these tasks in sequence.
    *   Add a **Tumbling Window Trigger** to schedule the master pipeline for regular, automated runs.

**6. Connecting Power BI**
*   Open Power BI Desktop and use the native `Azure Databricks` connector to connect to your cluster.
*   You will need the **Server Hostname** and **HTTP Path** from your cluster's JDBC/ODBC settings page in Databricks.
*   Authenticate using your Azure Active Directory account.
*   Once connected, navigate to your Unity Catalog tables, load the data, and start building your reports.

## What You Will Achieve

By completing this project, you will gain hands-on experience and proficiency in a wide range of modern data engineering skills:

*   **Cloud Data Engineering:** You will learn to architect and implement end-to-end data solutions on Microsoft Azure.
*   **Big Data Processing:** You will use Azure Databricks with PySpark and Spark SQL to transform large datasets efficiently.
*   **Lakehouse Architecture:** You will build and manage a reliable data platform with Delta Lake, implementing ACID transactions, time travel, and robust `MERGE` operations for incremental loads.
*   **Data Governance:** You will set up and use Unity Catalog for centralized access control, auditing, and data lineage.
*   **Data Orchestration:** You will build, schedule, and monitor resilient data pipelines with Azure Data Factory.
*   **BI and Reporting:** You will connect a leading BI tool like Power BI to a big data backend to enable self-service analytics for business users.
*   **Engineering Best Practices:** You will apply industry best practices for secure credential management (Azure Key Vault), idempotent pipeline design, and cost-effective cloud resource management.

