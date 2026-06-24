# 🏢 SQL Data Warehouse Project | Bronze → Silver → Gold Architecture

A complete **SQL Server Data Warehouse** implementation following the **Medallion Architecture (Bronze, Silver, Gold)**. This project demonstrates how raw CRM and ERP data can be ingested, cleansed, transformed, validated, and modeled into a business-ready analytical data warehouse using SQL Server.

---

## 📌 Project Overview

This project simulates a real-world Enterprise Data Warehouse (EDW) built entirely in SQL Server.

The pipeline follows three layers:

* **Bronze Layer** – Raw data ingestion from CSV files
* **Silver Layer** – Data cleansing, validation, transformation, and standardization
* **Gold Layer** – Business-ready dimensional model (Star Schema) for reporting and analytics

The project also includes:

* ETL Stored Procedures
* Data Quality Validation Scripts
* Error Handling
* Performance Logging
* Star Schema Design

---

## 🏗️ Architecture

```
                CSV Files
           (CRM & ERP Sources)
                    │
                    ▼
        ┌────────────────────┐
        │    Bronze Layer    │
        │ Raw Data Storage   │
        └────────────────────┘
                    │
         Stored Procedure ETL
                    │
                    ▼
        ┌────────────────────┐
        │    Silver Layer    │
        │ Cleansed & Standard│
        └────────────────────┘
                    │
         Business Transformations
                    │
                    ▼
        ┌────────────────────┐
        │     Gold Layer     │
        │ Star Schema Model  │
        └────────────────────┘
                    │
                    ▼
         Analytics / Reporting
```

---

# 📂 Project Structure

```
SQL-Data-Warehouse/
│
├── datasets/
│   ├── source_crm/
│   └── source_erp/
│
├── scripts/
│   ├── Bronze/
│   │   ├── create_bronze_tables.sql
│   │   └── load_bronze.sql
│   │
│   ├── Silver/
│   │   ├── create_silver_tables.sql
│   │   └── load_silver.sql
│   │
│   ├── Gold/
│   │   └── create_gold_views.sql
│   │
│   └── Quality_Checks/
│       ├── silver_quality_checks.sql
│       └── gold_quality_checks.sql
│
└── README.md
```

---

# 🛠 Technologies Used

* Microsoft SQL Server
* SQL Server Management Studio (SSMS)
* T-SQL
* Stored Procedures
* BULK INSERT
* Window Functions
* Views
* Star Schema
* Data Warehouse Modeling

---

# 📥 Bronze Layer

The Bronze layer stores **raw data exactly as received** from CRM and ERP source systems.

### Source Systems

### CRM

* Customer Information
* Product Information
* Sales Details

### ERP

* Customer Details
* Customer Locations
* Product Categories

### Features

* Raw Data Storage
* Bulk CSV Loading
* Minimal Transformations
* High-Speed Ingestion
* ETL Logging
* Error Handling using TRY...CATCH

---

# ⚙ Bronze ETL Process

The `bronze.load_bronze` stored procedure performs:

* Truncates existing tables
* Loads CSV files using BULK INSERT
* Measures execution time
* Logs execution status
* Handles exceptions gracefully

Example:

```sql
EXEC bronze.load_bronze;
```

---

# ✨ Silver Layer

The Silver layer cleans and standardizes raw data before analytics.

### Data Cleaning Includes

* Removing duplicate customers
* Trimming whitespace
* Standardizing gender values
* Standardizing marital status
* Cleaning country names
* Fixing invalid sales values
* Validating dates
* Calculating product end dates
* Handling NULL values
* Standardizing product categories

---

# ⚙ Silver ETL Process

The `silver.load_silver` procedure performs:

* Data Validation
* Data Cleansing
* Data Standardization
* Business Rule Enforcement
* Performance Logging

Example:

```sql
EXEC silver.load_silver;
```

---

# ⭐ Gold Layer

The Gold layer contains the final analytical model.

It is designed using a **Star Schema**.

## Dimension Tables

### dim_customers

Contains:

* Customer Information
* Country
* Gender
* Birth Date
* Customer Creation Date

---

### dim_products

Contains:

* Product Information
* Product Categories
* Product Line
* Product Cost
* Product Maintenance

---

### Fact Table

### fact_sales

Contains:

* Order Number
* Customer Key
* Product Key
* Order Date
* Ship Date
* Due Date
* Sales Amount
* Quantity
* Price

---

# 📊 Star Schema

```
                dim_customers
                      │
                      │
                      │
fact_sales ───────────┼────────── dim_products
```

---

# ✅ Data Quality Checks

The project includes dedicated quality validation scripts.

### Silver Layer Checks

* Duplicate Primary Keys
* NULL Values
* Invalid Dates
* Negative Prices
* Invalid Sales
* Data Standardization
* Leading & Trailing Spaces
* Product Date Validation

---

### Gold Layer Checks

* Surrogate Key Uniqueness
* Referential Integrity
* Missing Dimension Records
* Fact-Dimension Relationship Validation

---

# 📈 SQL Concepts Demonstrated

This project showcases practical SQL Server skills including:

* Stored Procedures
* BULK INSERT
* Window Functions
* ROW_NUMBER()
* LEAD()
* CASE Statements
* COALESCE()
* ISNULL()
* NULLIF()
* TRIM()
* REPLACE()
* SUBSTRING()
* Aggregate Functions
* Data Validation
* Error Handling
* TRY...CATCH
* Star Schema Design
* ETL Development

---

# 🚀 How to Run

## 1. Create Schemas

```sql
CREATE SCHEMA bronze;
CREATE SCHEMA silver;
CREATE SCHEMA gold;
```

---

## 2. Create Bronze Tables

Run:

```
create_bronze_tables.sql
```

---

## 3. Load Bronze Data

```sql
EXEC bronze.load_bronze;
```

---

## 4. Create Silver Tables

Run:

```
create_silver_tables.sql
```

---

## 5. Load Silver Data

```sql
EXEC silver.load_silver;
```

---

## 6. Create Gold Views

Run:

```
create_gold_views.sql
```

---

## 7. Execute Quality Checks

Run:

* silver_quality_checks.sql
* gold_quality_checks.sql

---

# 📊 Business Benefits

* Centralized Data Warehouse
* Standardized Data
* Improved Data Quality
* Faster Analytics
* Simplified Reporting
* Better Decision Making
* Reusable ETL Framework

---

# 🎯 Learning Outcomes

By completing this project, I gained hands-on experience in:

* Designing a SQL Data Warehouse
* Implementing Medallion Architecture
* Building ETL Pipelines
* Data Cleaning & Transformation
* Data Quality Validation
* Star Schema Modeling
* SQL Performance Logging
* Production-style SQL Development

---

# 👨‍💻 Author

**Aman Prajapati**

Aspiring Data Engineer passionate about building scalable data platforms, ETL pipelines, and cloud-based analytics solutions.

### Connect with me

* LinkedIn: https://www.linkedin.com/in/aman-prajapati-96a42328b/
* GitHub: https://github.com/amanprajapati10

---

## ⭐ If you found this project helpful, consider giving it a Star!
