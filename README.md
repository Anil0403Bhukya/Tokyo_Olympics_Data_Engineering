# Tokyo Olympics 2020 — End-to-End Data Engineering Project

An end-to-end data engineering project built using Azure Data Factory, Azure Data Lake Storage Gen2, Azure Databricks, PySpark, SQL, Unity Catalog, Databricks SQL Warehouse, and Power BI.

The project processes Tokyo Olympics 2020 athlete, coach, team, gender, and medal datasets through a layered data engineering pipeline and produces curated datasets for analytics and visualization.

---

## Project Architecture

```text
Source CSV Files
      |
      v
Azure Data Factory
      |
      v
Azure Data Lake Storage Gen2
      |
      +----------------+
      |  Raw Layer    |
      +----------------+
              |
              v
      Azure Databricks
              |
      +----------------+
      | Bronze Layer  |
      +----------------+
              |
              v
      Data Transformation
      using PySpark
              |
      +----------------+
      | Silver Layer  |
      +----------------+
              |
              v
      Gold Layer
              |
      +-----------------------------+
      | Country & Sport Analytics   |
      +-----------------------------+
              |
              v
        Unity Catalog
              |
              v
   Databricks SQL Warehouse
              |
              v
          Power BI
              |
              v
     Interactive Dashboard

```
##Technologies Used
Azure Data Factory
Azure Data Lake Storage Gen2
Azure Databricks
PySpark
SQL
Delta Lake
Unity Catalog
Databricks SQL Warehouse
Power BI
GitHub

##Source Data
The project uses Tokyo Olympics 2020 datasets containing:
Athletes
Coaches
EntriesGender
Medals
Teams

The raw CSV files are stored in:

Data/Raw/
├── Athletes.csv
├── Coaches.csv
├── EntriesGender.csv
├── Medals.csv
└── Teams.csv

##Data Pipeline
Azure Data Factory is used to orchestrate the ingestion of the Olympic datasets into Azure Data Lake Storage Gen2.
The ingestion pipeline processes:
Athletes
Coaches
EntriesGender
Medals
Teams

The ADF pipeline definition is maintained in the repository under:

ADF/

##2. Bronze Layer
The ingested source datasets are stored in the Bronze layer in Delta format.
The Bronze layer provides the initial persisted version of the data before transformation and further processing.

##3. Silver Layer

The Silver layer contains cleaned and deduplicated datasets processed using PySpark in Azure Databricks.
Transformation activities include:
Removing duplicate records
Applying data transformations
Standardizing the datasets
Handling schema-related issues
Renaming Rank by Total to Rank_by_Total
Writing transformed datasets in Delta format

Silver datasets include:

silver/
├── athletes
├── coaches
├── gender
├── medals
└── teams

The main Databricks transformation notebook is available at:
Notebooks/Tokyo_Olympic_Transformation.py

##4. Gold Layer

The Gold layer contains analytics-ready datasets prepared for reporting and business analysis.
The main Country dataset contains:
Country
Gold
Silver
Bronze
Total
Rank
Total_Players

Olympic ranking is based on:

Gold → Silver → Bronze

The project also creates a Sport dataset containing the number of athletes by discipline.

##Unity Catalog

Unity Catalog is used to register and manage the curated analytical datasets.
The project uses the:
2020_olympics
└── default
    ├── country
    └── sport

The country table provides country-level medal and athlete statistics.
The sport table provides athlete counts by sport/discipline.

##Databricks SQL Warehouse

A serverless Databricks SQL Warehouse is used to query the curated Gold datasets.

The SQL Warehouse provides the analytical layer used to connect the processed Olympic data with Power BI.

##Power BI Dashboard

The final Power BI dashboard provides interactive analysis of the Tokyo Olympics 2020 data.

Dashboard Features
Total Medals KPI
Gold Medals KPI
Silver Medals KPI
Bronze Medals KPI
Selected Country Rank
Total Players
Country slicer with search
Number of Athletes by Sport
Gold, Silver and Bronze by Country
Medal distribution donut chart
Total Medals by Country
Total Athletes by Country
Country by Gold analysis

The dashboard supports country-level filtering for dynamic analysis of medal and athlete statistics.

##Key Results

The processed Olympic dataset contains:

Metric	Value
| Metric        |  Value |
| ------------- | -----: |
| Total Medals  |  1,080 |
| Gold Medals   |    340 |
| Silver Medals |    338 |
| Bronze Medals |    402 |
| Total Players | 10,204 |


##Example Country Analysis

| Country       | Gold | Silver | Bronze | Total | Rank |
| ------------- | ---: | -----: | -----: | ----: | ---: |
| USA           |   39 |     41 |     33 |   113 |    1 |
| China         |   38 |     32 |     18 |    88 |    2 |
| Japan         |   27 |     14 |     17 |    58 |    3 |
| Great Britain |   22 |     21 |     22 |    65 |    4 |
| ROC           |   20 |     28 |     23 |    71 |    5 |

##Data Engineering Concepts Demonstrated

This project demonstrates practical implementation of:

Cloud data ingestion
ETL pipeline orchestration
Azure Data Lake architecture
Medallion architecture
Bronze, Silver and Gold data layers
PySpark data transformation
Data deduplication
Delta Lake
Azure Data Factory
Unity Catalog
SQL analytics
Databricks SQL Warehouse
Power BI integration
Interactive data visualization
GitHub version control
End-to-end cloud data engineering

##Repository Structure
Tokyo_Olympics_Data_Engineering/
│
├── ADF/
│   ├── ARMTemplateForFactory.json
│   └── ARMTemplateParametersForFactory.json
│
├── Data/
│   └── Raw/
│       ├── Athletes.csv
│       ├── Coaches.csv
│       ├── EntriesGender.csv
│       ├── Medals.csv
│       └── Teams.csv
│
├── Notebooks/
│   └── Tokyo_Olympic_Transformation.py
│
├── .gitignore
├── LICENSE
└── README.md

##Project Objective

The objective of this project is to demonstrate an end-to-end modern data engineering workflow using Azure cloud services.
The pipeline takes raw Olympic datasets through ingestion, storage, transformation, curation, governance, analytics, and visualization.
The final solution enables interactive analysis of Olympic medals, rankings, countries, athletes, and sports through Power BI.
