# 📦 Data Warehouse Project

## Project Overview
This project demonstrates end-to-end data warehouse development, from raw data ingestion to business-ready analytics. Built using SQL Server and Power BI, it showcases data engineering practices ETL pipelines, dimensional modeling, and comprehensive data quality management.

**Key Focus Areas**:
- Medallion Architecture implementation (Bronze-Silver-Gold)
- Automated ETL with T-SQL stored procedures
- Data quality analysis and transformation
- Star schema dimensional modeling
- Power BI dashboard development

---

## Project Structure

```
 📦 data-warehouse-project
 │
 ├── 📁 analytics/
 │   ├── 📄 DAX.md
 │   ├── 📄 README.md
 │   └── 📄 dashboard.md
 │
 ├── 📁 datasets/
 │   ├── 📁 crm/
 │   │   ├── 📄 cust_info.csv
 │   │   ├── 📄 prd_info.csv
 │   │   └── 📄 sales_details.csv
 │   └── 📁 erp/
 │       ├── 📄 CUST_AZ12.csv
 │       ├── 📄 LOC_A101.csv
 │       └── 📄 PX_CAT_G1V2.csv
 │
 ├── 📁 sql_scripts/
 │   │
 │   ├── 📁 01_database_initialization/
 │   │   └── 📄 01_init_database.sql
 │   │
 │   ├── 📁 02_bronze_layer/
 │   │   ├── 📁 02a_table_scripts/
 │   │   │   ├── 📄 01_bronze_dll.sql
 │   │   │   └── 📄 02_bronze_log.sql
 │   │   │
 │   │   └── 📁 02b_stored_procedures/
 │   │       ├── 📄 01_sp_bronze.load_crm_cust_info.sql
 │   │       ├── 📄 02_sp_bronze.load_crm_prd_info.sql
 │   │       ├── 📄 03_sp_bronze.load_crm_sales_details.sql
 │   │       ├── 📄 04_sp_bronze.load_erp_cust_az12.sql
 │   │       ├── 📄 05_sp_bronze.load_erp_loc_a101.sql
 │   │       ├── 📄 06_sp_bronze.load_erp_px_cat_g1v2.sql
 │   │       └── 📄 07_sp_load_bronze_master.sql
 │   │
 │   ├── 📁 03_silver_layer/
 │   │   ├── 📁 01_eda/
 │   │   │   ├── 📄 crm_customer.sql
 │   │   │   ├── 📄 crm_prd_info.sql
 │   │   │   ├── 📄 crm_sales_details.sql
 │   │   │   ├── 📄 erp_cust_az12.sql
 │   │   │   ├── 📄 erp_loc_a101.sql
 │   │   │   └── 📄 erp_px_cat_g1v2.sql
 │   │   │
 │   │   ├── 📁 02_table_scripts/
 │   │   │   ├── 📄 silver_indexing.sql
 │   │   │   ├── 📄 silver_log.sql
 │   │   │   └── 📄 silver_table_creation.sql
 │   │   │
 │   │   ├── 📁 03_cleansing_load/
 │   │   │   ├── 📄 crm_cust_info.sql
 │   │   │   ├── 📄 crm_prd_info.sql
 │   │   │   ├── 📄 crm_sales_details.sql
 │   │   │   ├── 📄 erp_cust_az12.sql
 │   │   │   ├── 📄 erp_loc_a101.sql
 │   │   │   └── 📄 erp_px_cat_g1v2.sql
 │   │   │
 │   │   └── 📁 04_stored_procedures/
 │   │       ├── 📄 01_sp_load_crm_customer.sql
 │   │       ├── 📄 02_sp_load_crm_product.sql
 │   │       ├── 📄 03_sp_load_crm_sales.sql
 │   │       ├── 📄 04_sp_load_erp_customer.sql
 │   │       ├── 📄 05_sp_load_erp_location.sql
 │   │       ├── 📄 06_sp_load_erp_product_category.sql
 │   │       └── 📄 07_sp_load_silver_master.sql
 │   │
 │   └── 📁 05_gold_layer/
 │       ├── 📄 dim_customers.sql
 │       ├── 📄 dim_products.sql
 │       └── 📄 fact_sales.sql
 │
 ├── 📄 LICENSE
 └── 📄 README.md
```

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
