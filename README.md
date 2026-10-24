# Decathlon End-to-End Data Engineering Project

## Project Overview

This project implements an end-to-end data engineering pipeline for Decathlon retail data. It covers data ingestion, data quality checks, change data capture (CDC), data cleaning, PII protection, data normalization, and analytical data modeling.

The project uses **Python and Pandas for data processing** and Databricks with Delta tables for data storage and organization.

## Technologies Used

- **Python** – Data processing and pipeline logic
- **Pandas** – Data cleaning, transformation, validation, and manipulation
- **SQL** – Data querying and validation
- **Databricks** – Cloud-based data engineering environment
- **Delta Lake** – Reliable table storage and data management
- **AWS S3** – Raw data storage and ingestion source
- **Git and GitHub** – Version control
- **Tableau** – Data visualization and dashboards

## Project Architecture

```text
AWS S3 (Raw Data)
       |
       v
Bronze Layer
       |
       v
CDC Layer
       |
       v
Silver Processed Layer
(Python + Pandas)
       |
       v
Silver Normalized Layer
(Relational Tables)
       |
       v
Gold Layer
(Fact and Dimension Tables)
       |
       v
Tableau Dashboards
```

## Data Sources

The project processes data from eight management areas:

1. Customer Management
2. Product Management
3. Sales and Commerce Management
4. Supply Chain Management
5. Marketing Management
6. Risk, Security, and Fraud Management
7. Corporate Management
8. After-Sales and Service Management

## Project Layers

### 1. Raw Data Ingestion

- Ingest raw management CSV files from AWS S3.
- Organize files by business date.
- Preserve source file information for traceability.

### 2. Bronze Layer

- Store ingested source data in Bronze Delta tables.
- Preserve original source columns and ingestion metadata.
- Maintain the initial representation of the source data.

### 3. Change Data Capture (CDC)

- Detect INSERT and UPDATE events using configured business keys and tracked attributes.
- Store change history in dedicated CDC tables.
- Maintain a processing log to support reruns.
- Document limitations caused by synthetic data and unreliable identity attributes.

### 4. Silver Processed Layer

Use **Python and Pandas** to process the eight management datasets.

- Clean whitespace and standardize values.
- Convert columns to appropriate data types where safe.
- Handle missing values and mixed-type columns carefully.
- Apply approved HMAC-SHA-256 hashing to sensitive identifiers.
- Validate data types, required fields, numeric values, and business rules.
- Generate data-quality reports.
- Preserve source metadata and data traceability.

### 5. Silver Normalized Layer

Organize processed data into relational entities based on business meaning.

Potential entities include:

- Customers and customer addresses
- Products, brands, and categories
- Orders, payments, and shipments
- Suppliers, warehouses, and inventory
- Campaigns and advertisements
- Employees, departments, and stores
- Risk cases and events
- Service cases and warranties

Apply primary-key and foreign-key validation, check referential integrity, and document invalid relationships. Create only tables supported by the actual source data.

### 6. Gold Layer

Build analytical fact and dimension tables for reporting.

- Fact tables for relevant business processes.
- Dimension tables for customers, products, dates, stores, and other supported dimensions.
- Validate keys and relationships before making data available for reporting.

### 7. Visualization

Connect Tableau to Databricks SQL Warehouse to build dashboards for sales, products, customer activity, inventory, marketing performance, and service operations where supported by the available data.

## Data Quality and Reliability

The pipeline includes:

- Schema and data-type validation
- Duplicate-key checks
- Missing-value checks
- Foreign-key and orphan-record checks
- Business-rule validation
- CDC rerun and idempotency checks
- Source-to-target row-count reconciliation
- Data-quality issue reporting

Invalid records are reported rather than silently removed or modified.

## Security

- Use Databricks secrets for sensitive credentials and HMAC keys.
- Never hardcode credentials in notebooks or GitHub.
- Apply approved PII hashing policies.
- Preserve business keys when needed for relationships.
- Restrict access to sensitive data.

## Repository Structure

```text
Decathlon-Data-Engineering/
├── notebooks/
│   ├── bronze_ingestion
│   ├── cdc_processing
│   ├── silver_processing
│   ├── silver_normalization
│   └── gold_modeling
├── src/
│   ├── ingestion/
│   ├── cdc/
│   ├── silver/
│   └── gold/
├── data_quality_reports/
├── docs/
│   └── architecture.md
└── README.md
```

*Adjust the folder structure to match the files actually present in the repository.*

## Expected Outcomes

- A structured multi-layer data pipeline.
- Cleaned and validated management datasets.
- CDC history with rerun protection.
- Normalized relational tables with key validation.
- Analytical fact and dimension tables.
- Data-quality reports and traceable processing.
- Tableau-ready datasets.

## Disclaimer

This project uses the available Decathlon-related datasets. Data-quality findings and CDC changes must be interpreted according to the actual source data; detected changes do not necessarily represent genuine business events.
