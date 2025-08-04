# End-to-End Data Warehouse Pipeline: From MySQL to Snowflake

This repository contains the code and configuration for a fully automated data pipeline that integrates transactional data from a **MySQL** database into an analytical **Data Warehouse** hosted in **Snowflake**. The pipeline performs extraction, transformation, and loading (ETL) using **Python** and orchestrates the entire workflow with **Apache Airflow**, running inside a reproducible **Docker** environment.

Once the cleaned data is loaded into Snowflake, it is modeled into a **star schema** using SQL scripts. A business dashboard was also developed in **Power BI**, connected directly to Snowflake via **DirectQuery**, to enable real-time monitoring of key metrics.

> ⚠️ This solution was implemented for a real company. For confidentiality reasons, the original database and data files are not included in this repository.
  
---
## Objectives

The main objective of this project is to implement a scalable and automated data pipeline that transforms transactional ERP data into a reliable analytical foundation for business decision-making.

This solution was designed for a real-world company that required:

- Centralized access to cleaned and structured sales and operational data.
- Automation of weekly data refresh processes.
- Integration with a modern cloud-based Data Warehouse (**Snowflake**).
- Real-time business intelligence through **Power BI** dashboards.
- A maintainable and secure architecture using industry best practices.

## Table of contents

1. [Dataset and data sources](#dataset-and-data-sources)
2. [Technologies used](#technologies-used)
3. [Pipeline architecture](#pipeline-architecture)
4. [Data modelling](#data-modelling)
5. [ETL description](#etl-description)
6. [Orchestration with Airflow](#orchestration-with-airflow)
7. [Environment setup](#environment-setup)
8. [Running the project](#running-the-project)

## Dataset and Data Sources

This pipeline was developed for **Glamour Perú**, a direct sales fashion company. The main data source is a transactional **MySQL** database that stores sales and operational data. The following tables are extracted:

| Table                | Description                                             |
|----------------------|---------------------------------------------------------|
| **persona**          | Personal information of sellers and coordinators        |
| **contenido**        | Product catalog with detailed attributes                |
| **campana**          | Campaign metadata (e.g., number, start/end dates)       |
| **documento**        | Sales orders at the header level                        |
| **documentodetalle** | Sales orders at the line-item level                     |

To enrich the raw data, two auxiliary Excel files are also used during preprocessing:

- `tabla_cruzada.xlsx`: Maps location codes to human-readable descriptions  
- `dimdate.xlsx`: Provides a complete date dimension with calendar attributes

These sources are processed and integrated to build an analytical foundation for the company's reporting and business intelligence needs.

## Technologies Used

| Category           | Tools / Libraries                                                                 |
|--------------------|-----------------------------------------------------------------------------------|
| **Languages**      | Python 3.10, SQL                                                                  |
| **Data Extraction**| [`pymysql`](https://pypi.org/project/PyMySQL/) – connects to the MySQL source     |
| **Transformation** | `pandas`, `numpy`, `openpyxl` – used for data cleaning, manipulation, and enrichment |
| **Data Loading**   | `snowflake-connector-python`, `write_pandas` from `snowflake.connector.pandas_tools` – used to load DataFrames into Snowflake |
| **Orchestration**  | Apache Airflow 2.5.1 – uses PythonOperator for ETL logic and SnowflakeOperator for executing SQL scripts |
| **Containers**     | Docker & Docker Compose – for isolated, reproducible deployment of Airflow (with Postgres and Redis services) |
| **Data Warehouse** | Snowflake – cloud-based data warehouse for storing star-schema modeled data        |

## Pipeline Architecture

The ETL pipeline is designed in modular stages to transform raw ERP transactional data into an analytical **star schema** hosted in **Snowflake**, with orchestration managed by **Apache Airflow**. It follows a weekly schedule and includes the following components:

> **Figure of overall pipeline architecture here**

### 1. Extraction from MySQL

The `extraer_datos_mysql` function connects to the transactional **MySQL** database using credentials defined in the `.env` file. It extracts five raw tables:

- `persona` (person)
- `contenido` (product)
- `campana` (campaign)
- `documento` (order header)
- `documentodetalle` (order lines)

Each table is returned as a `pandas` `DataFrame` object for further processing.

### 2. Data Transformation and Cleansing

The `limpiar_tablas` function applies several transformations:

- **Column removal and renaming:** Irrelevant fields are dropped and column names are standardized.
- **Product classification:** A dictionary-based keyword mapping is used to assign each product to a category and subcategory.
- **Dimension creation:** Six dimension tables are created — `campana`, `contenido`, `ubicacion`, `fecha`, `vendedor`, and `coordinadora` — as well as two staging tables: `STG_DOCUMENTO` and `STG_DOCUMENTODETALLE`.

This step uses two auxiliary files:
- `tabla_cruzada.xlsx`: Maps location codes to readable descriptions.
- `dimdate.xlsx`: Provides a complete calendar dimension.

### 3. Loading to Snowflake

The `cargar_tablas_en_snowflake` function iterates over all transformed DataFrames and loads them into Snowflake using `write_pandas`. All connection parameters (`SNOWFLAKE_USER`, `SNOWFLAKE_PASSWORD`, `SNOWFLAKE_ACCOUNT`, etc.) are sourced from the `.env` file. Tables are overwritten at each run to ensure a clean and consistent load.

### 4. Fact Table Construction

Once the dimension and staging tables are available in Snowflake, SQL scripts are executed to build the fact tables:

- `FACT_PEDIDOS`: Combines `STG_DOCUMENTO` with dimension tables to calculate order-level metrics such as total amount, amount paid, and a new-seller flag.
- `FACT_PEDIDOS_DETALLE`: Combines `STG_DOCUMENTODETALLE` with `FACT_PEDIDOS` and `DIM_CONTENIDO` to calculate line-item quantities and amounts.

The scripts also handle campaign ordering and cleanup of temporary staging tables.

### 5. Pipeline Orchestration

A weekly **Apache Airflow DAG** orchestrates the full process:

1. Executes the Python-based ETL process (`etl_script.py`)
2. Triggers SQL scripts in Snowflake to build the fact tables

The DAG is designed to run only for the current campaign/week, without backfilling historical periods.

### 6. Business Intelligence Layer

A **Power BI** dashboard connects to Snowflake via **DirectQuery**. It provides real-time monitoring and visualization of key business indicators, including:

- Total revenue and payment rates
- Active sellers per campaign
- Churn behavior over time
- Order metrics and product breakdowns

This final layer enables business users at **Glamour Perú** to make informed decisions based on centralized, up-to-date information.

## Data modelling

The data is organised in a **star schema** to optimise analytical queries.  Six dimension tables and two fact tables are used.
> **Figure of starc schema here**

