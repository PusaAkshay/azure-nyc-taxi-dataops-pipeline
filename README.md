# Azure-nyc-taxi-dataops-pipeline

# 🚕 Azure NYC Taxi DataOps Pipeline

An end-to-end Data Engineering & DataOps pipeline built on Microsoft Azure that ingests NYC taxi trip data from a public API, processes it using scalable data transformation techniques, and delivers analytical insights through dashboards.

This project also demonstrates modern DataOps practices using Databricks Asset Bundles for seamless multi-environment deployment (Dev & Prod).

This project implements an end-to-end batch data pipeline for NYC Taxi data using... Azure Data Factory, Databricks (DLT + Autoloader), Unity Catalog, and DataOps deployment via Databricks Asset Bundles.

---

## 💡 Why This Project?

This project demonstrates how modern data platforms handle:
- Scalable ingestion of semi-structured data
- Incremental processing using Autoloader
- Reliable ETL with Delta Live Tables
- Data governance using Unity Catalog
- CI/CD-style deployments with Databricks Asset Bundles

It simulates a real-world production-grade analytics pipeline.
📅 **Dataset Used:** January 2024 (Single Month)

---

## 🏗️ Architecture
![Architecture](https://github.com/PusaAkshay/azure-nyc-taxi-dataops-pipeline/blob/919336505f7238e8beabb87a314b03d0966cfd01/Architecture.png)

---

## 🔄 Pipeline Flow

1. **Data Source**  
   NYC Taxi API (Parquet format – Jan 2024)

2. **Ingestion (Azure Data Factory)**  
   - Fetches monthly dataset  
   - Stores raw data in ADLS Gen2  

3. **Raw Storage (ADLS Gen2)**  
   - Container: `nyctaxi/YYYY/MM/*.parquet`  
   - Immutable raw data  

4. **Processing (Azure Databricks Jobs)**  
   - **Task 1: Autoloader**  
     - Incremental ingestion  
     - Converts Parquet → Bronze Delta Table  

   - **Task 2: DLT Pipeline (ETL)**  
     - Bronze → Silver (cleaned & validated)  
     - Silver → Gold (aggregated & analytics-ready)  

5. **Visualization**  
   Databricks Lakeview Dashboard (NYC Taxi Analytics)

---

## 🥉🥈🥇 Medallion Architecture

| Layer   | Storage Location | Description |
|---------|-----------------|-------------|
| Raw     | raw (ADLS)      | Original NYC Taxi Parquet files — never modified |
| Bronze  | bronze (ADLS)   | Ingested using Autoloader → stored as Delta (raw schema preserved) |
| Silver  | Unity Catalog (external location: silver) | Cleaned & validated data (null handling, schema enforcement) |
| Gold    | Unity Catalog (external location: gold)   | Aggregated & analytics-ready data |

Unity Catalog is configured with the silver ADLS container as the external location.  
DLT tables (`bronze_nyctaxi_trips`, `silver_nyctaxi_trips`, `gold_nyctaxi_trips`) are governed under environment-specific schemas:

- Dev: `nyctaxi.dev_nyctaxi_db`
- Prod: `nyctaxi.prod_nyctaxi_db`

---

## ⚙️ Tech Stack

| Tool | Purpose |
|------|---------|
| NYC Taxi API | Source of taxi trip data (Parquet format – Jan 2024 sample) |
| Azure Data Factory | Pipeline orchestration — data ingestion & scheduling |
| ADLS Gen2 | Scalable data lake storage (raw, bronze, silver) |
| Azure Databricks | Data processing using PySpark, Autoloader & Jobs |
| Autoloader | Incremental ingestion from raw → Bronze Delta tables |
| Delta Live Tables (DLT) | Declarative ETL pipeline (Bronze → Silver → Gold) |
| Delta Lake | Storage layer enabling ACID transactions & schema evolution |
| Unity Catalog | Centralized data governance (catalog, schemas, external locations) |
| Databricks Lakeview Dashboard | Visualization of NYC Taxi insights |
| Databricks Asset Bundles | Deployment across Dev & Prod environments |
| GitHub | Version control for code and bundle configurations |

---

## 📂 Repository Structure
```
NYC-Taxi-DataOps-Pipeline/
├── ADF/
│   ├── pipelines/
│   ├── datasets/
│   ├── linked_services/
│   └── factory/
├── Databricks/
│   ├── autoloader.ipynb
│   ├── etl.py
│   ├── nyc_taxi_dashboard.lvdash.json
│   └── bundle/
│       ├── dev/.bundle/nyc_taxi/dev
│       └── prod/.bundle/nyc_taxi/prod
└── images/
    ├── architecture.png
    ├── dlt_pipeline.png
    ├── job_run.png
    ├── dashboard.png
    └── databricks_bundle_execution_success.png
```
    
---

## ⚙️ Detailed Execution Steps

### Step 1 — Data Ingestion (Azure Data Factory)
* Fetches NYC Taxi data (Jan 2024) from public API.
* Stores raw data in ADLS Gen2 (Parquet).


---

### Step 2 — Raw to Bronze (Autoloader)
* Incremental ingestion from ADLS → Bronze Delta tables.

---

### Step 3 — Bronze to Silver (DLT Pipeline)
* Cleans and validates data.
* Applies expectations for data quality.

---

### Step 4 — Silver to Gold (Aggregation Layer)
![DLT Pipeline](https://github.com/PusaAkshay/azure-nyc-taxi-dataops-pipeline/blob/d46d6f4b5ae4cc922998334a37a251687dc209b0/DLT_Pipeline.png)

* Aggregated insights: trips, revenue, distance categories, etc.

---

### Step 5 — Pipeline Orchestration (Databricks Jobs)
![Databricks Job](https://github.com/PusaAkshay/azure-nyc-taxi-dataops-pipeline/blob/919336505f7238e8beabb87a314b03d0966cfd01/JOB.png)

* Orchestrates Autoloader, DLT Pipeline, Dashboard refresh.

---

### Step 6 — Data Visualization (Dashboard)
![Dashboard](https://github.com/PusaAkshay/azure-nyc-taxi-dataops-pipeline/blob/6aed389f55aab9c47134250306660e13ebc96612/Dashboard.png)

Built using Databricks Lakeview Dashboard connected to curated NYC Taxi Gold Layer data.

**Visuals:**
- 🔢 KPI Cards — Total Trips, Total Revenue, Avg Trip Duration, Avg Trip Distance  
- 📊 Trips by Time of Day — Morning, Afternoon, Evening, Night  
- 📈 Trips by Hour — Hourly demand trends  
- 🥧 Revenue by Payment Type — Credit Card, Cash, Other modes  
- 📏 Trip Distance Distribution — Short, Medium, Long, Very Long  
- 💰 Cost Efficiency — Efficiency comparison across trip categories  
- 🔵 Trip Duration vs Distance — Scatter plot patterns & outliers  
## 📊 Data Metrics & Scale

The pipeline processes real-world NYC Taxi data (Jan 2024) at scale:

- 🚕 **Total Trips:** ~2.72 Million  
- 💰 **Total Revenue:** ~$74.67 Million  
- ⏱️ **Average Trip Duration:** ~15.76 minutes  
- 📏 **Average Trip Distance:** ~21.42 miles 

### 📈 Analytical Insights Generated

- Peak demand observed during afternoon & evening hours  
- Majority of trips fall under **medium distance category**  
- Credit card is the dominant payment method  
- Clear correlation between trip distance and duration with identifiable outliers  

---

## 🚕 Databricks Bundle Execution
![Databricks Bundle Execution](https://github.com/PusaAkshay/azure-nyc-taxi-dataops-pipeline/blob/919336505f7238e8beabb87a314b03d0966cfd01/databricks_bundle_execution_success.png)

Successfully executed both **Dev** and **Prod** environments using Databricks bundles.  
Screenshot shows successful runs in Databricks Jobs & Pipelines UI.

---

## 📂 Data Source

NYC Taxi & Limousine Commission (TLC) dataset:  
- Base URL: https://d37ci6vzurychx.cloudfront.net  
- File: `/trip-data/yellow_tripdata_2024-01.parquet`

---
## 🚧 Challenges & Learnings

- Managing incremental ingestion with Autoloader
- Designing DLT expectations for data quality
- Setting up Unity Catalog external locations
- Debugging multi-environment bundle deployments


