# Brazilian E-Commerce Data Engineering Project

This project implements an ETL pipeline using Airflow, PostgreSQL, and Python to process and analyze the Brazilian E-Commerce Public Dataset by Olist.

## Project Overview

This solution extracts e-commerce data from CSV files, loads it into a PostgreSQL database, and transforms it to calculate meaningful business KPIs. The project is fully containerized using Docker for easy setup and reproducibility.

### Architecture

![Architecture Diagram](https://raw.githubusercontent.com/username/ecommerce-data-engineering/main/architecture.png)

The project consists of:
- PostgreSQL database for data storage
- Airflow for orchestration and scheduling
- Python with pandas for data processing
- Matplotlib and Seaborn for visualizations

## Key Features

1. **Automated Data Ingestion Pipeline**
   - ETL process to load CSV data into PostgreSQL
   - Proper error handling and logging
   - Handles various data types and transformations

2. **Business KPI Calculation**
   - Customer Lifetime Value (CLV) analysis
   -