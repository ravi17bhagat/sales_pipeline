# Sales pipeline (Dagster)

## Overview
This project is a Dagster-based data pipeline that orchestrates data ingestion, transformation, and loading using SQLite3 as the target database. The pipeline includes partitioned assets and ensures reliable data processing with testing.

## Project Structure
```sh
sales_pipeline/
│-- assets.py             # Dagster assets
│-- jobs.py               # Job definitions
│-- repository.py         # Dagster project definitions
│-- db.sqlite3            # Sqlite3 DB
│-- tests/                # Unit Tests for assets
│-- requirements.txt      # Dependencies
```

## Prerequisites
Ensure you have the following installed:
- Python (>=3.8)
- pip

## Installation

```sh
pip install -r requirements.txt
```

## Steps for execution

1. Navigate to root directory of the project
2. Execute below command to run dagster server locally
```sh
dagster dev --module-name repository
```
3. The Dagster UI will start and be accessible at http://localhost:3000.

## Executing the pipeline

1. Navigate to Dagster UI at http://localhost:3000
2. Go to **Jobs** tab from Navigation pane and select **load_sales_data** pipeline.
3. Click on **Materialize all** and select **2025-03-01-12:00** partition.
4. Click on **Launch run** to execute the pipeline.
5. Go to **Runs** tab from navigation pane to monitor the execution.
6. Once execution is successful, the **Order**, **Products** and **Sales** tables will be materialized.

## Validating data in Sqlite3 DB

1. Open console and navigate to project root directory.
2. Execute below commands to check the materialized data
```sh
sqlite3 db.sqlite3
.tables
```
3. **Order**, **Products** and **Sales** tables should be listed, use sql queries to check data
```sh
select * from orders;
select * from products;
select * from sales;
```

## Running Tests

1. Navigate to root directory of the project
2. Execute unit tests using below command
```sh
pytest
```