# Sales pipeline (Dagster)

## Overview
This project is a Dagster-based data pipeline that orchestrates data ingestion, transformation, and loading using SQLite3 as the target database. The pipeline includes partitioned assets and ensures reliable data processing with testing.

## Project Structure
```sh
sales_pipeline/
│-- assets.py             # Dagster assets
│-- jobs.py               # Job definitions
│-- repository.py         # Dagster project definitions
│-- tests/                # Test cases for assets and jobs
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
