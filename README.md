# medallion-dwh-sales-analytics

End-to-end sales data warehouse using Medallion Architecture with SQL Server ETL and Power BI dashboards.

---

## Project Overview

This project demonstrates end-to-end data warehouse development, from raw data ingestion to business-ready analytics. Built using SQL Server and Power BI, it showcases data engineering practices ETL pipelines, dimensional modeling, and comprehensive data quality management.

**Key Focus Areas**:
- Medallion Architecture implementation (Bronze-Silver-Gold)
- Automated ETL with T-SQL stored procedures
- Data quality analysis and transformation
- Star schema dimensional modeling
- Power BI dashboard development

---

## Architecture

The project follows a three-layer architecture:

- **🥉 Bronze Layer** - Raw data ingestion from CSV files preserving source structure

- **🥈 Silver Layer** - Data cleansing, validation, and business rule application

- **🥇 Gold Layer** - Analytics-optimized star schema for reporting


```
CSV Files → Bronze (Raw) → Silver (Cleaned) → Gold (Star Schema) → Power BI
```

---

## Technical Implementation

- **Database Platform**: Microsoft SQL Server 2019+

- **ETL Approach**: T-SQL stored procedures with master orchestration

- **Data Model**: Star schema with 2 dimensions and 1 fact table

- **BI Tool**: Power BI Desktop with DAX measures

- **Refresh Strategy**: Full load with TRUNCATE + INSERT pattern

---

## Data Pipeline

### 🥉 Bronze Layer

| Table | Source | Records | Description |
|-------|--------|---------|-------------|
| bronze.crm_cust_info | CRM | 18,493 | Customer master data |
| bronze.crm_prd_info | CRM | 397 | Production catalog |
| bronze.crm_sales_details | CRM | 60,398 | Sales transactions |
| bronze.erp_cust_az12 | ERP | 18,483 | Customer demographics |
| bronze.erp_loc_a101 | ERP | 18,484 | Customer locations |
| bronze.erp_px_cat_g1v2 | ERP | 37 | Product categories |

### 🥈 Silver Layer

| Table | Records | Key Transformations |
|-------|---------|---------------------|
| silver.crm_cust_info | 18,484 | Deduplication, code expansion |
| silver.crm_prd_info | 397 | SCD Type 2, category parsing |
| silver.crm_sales_details | 60,398 | Date imputation, business logic validation |
| silver.erp_cust_az12 | 18,483 | Key standardization, date validation |
| silver.erp_loc_a101 | 18,484 | Key standardization, country expansion |
| silver.erp_px_cat_g1v2 | 37 | Direct transfer (clean source) |

### 🥇 Gold Layer

| Object | Type | Records | Description |
|--------|------|---------|-------------|
| gold.dim_customers | Dimension | 14,484 | Customer master |
| gold.dim_products | Dimension | 295 | Current product versions |
| gold.fact_sales | Fact | 60,398 | Sales transactions |

---

## Key Features

**Data Quality**
- Comprehensive EDA with documented findings and decisions
- Discovered 7-day shipping pattern for date imputation
- Cross-system key standardization across 3 sources
- Business rule validation for sales calculations

**Implementation Techniques**
- SCD Type 2 implementation for product versioning
- Automated ETL with error handling and load logging
- Surrogate key generation
- View-based Gold layer for real-time access

**Analytics**
- Power BI dashboard
- Profitability metrics with cost and margin calculations
- DAX measures for business KPIs

---

## Usage

Execute scripts in order by layer to build the complete data warehouse.
