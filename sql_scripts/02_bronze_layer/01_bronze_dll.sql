-- =============================================
-- Bronze Layer: Table Creation
-- =============================================
-- Purpose: Create Bronze layer staging tables for raw data
-- Creates: 6 Bronze tables (3 CRM + 3 ERP)
-- Run Order: After database initialization (01_init_database.sql)
-- =============================================

-- =============================================
-- Note: All columns use NVARCHAR(100) to accept raw data
--       without type conversion errors. Data types will be
--       enforced in the Silver layer transformations.
-- =============================================

USE DataWarehouse;
GO

-- =============================================
-- Step 1: Create CRM Tables
-- =============================================

DROP TABLE IF EXISTS bronze.crm_cust_info;
GO

CREATE TABLE bronze.crm_cust_info(
    cst_id NVARCHAR(100),
    cst_key NVARCHAR(100),
    cst_firstname NVARCHAR(100),
    cst_lastname NVARCHAR(100),
    cst_marital_status NVARCHAR(100),
    cst_gndr NVARCHAR(100),
    cst_create_date NVARCHAR(100)
);
GO

DROP TABLE IF EXISTS bronze.crm_prd_info;
GO

CREATE TABLE bronze.crm_prd_info(
    prd_id NVARCHAR(100),
    prd_key NVARCHAR(100),
    prd_nm NVARCHAR(100),
    prd_cost NVARCHAR(100),
    prd_line NVARCHAR(100),
    prd_start_dt NVARCHAR(100),
    prd_end_dt NVARCHAR(100)
);
GO

DROP TABLE IF EXISTS bronze.crm_sales_details;
GO

CREATE TABLE bronze.crm_sales_details(
    sls_ord_num NVARCHAR(100),
    sls_prd_key NVARCHAR(100),
    sls_cust_id NVARCHAR(100),
    sls_order_dt NVARCHAR(100),
    sls_ship_dt NVARCHAR(100),
    sls_due_dt NVARCHAR(100),
    sls_sales NVARCHAR(100),
    sls_quantity NVARCHAR(100),
    sls_price NVARCHAR(100)
);
GO

-- =============================================
-- Step 2: Create ERP Tables
-- =============================================

DROP TABLE IF EXISTS bronze.erp_cust_az12;
GO

CREATE TABLE bronze.erp_cust_az12(
    CID NVARCHAR(100),
    BDATE NVARCHAR(100),
    GEN NVARCHAR(100)
);
GO

DROP TABLE IF EXISTS bronze.erp_loc_a101;
GO

CREATE TABLE bronze.erp_loc_a101(
    CID NVARCHAR(100),
    CNTRY NVARCHAR(100)
);
GO

DROP TABLE IF EXISTS bronze.erp_px_cat_g1v2;
GO

CREATE TABLE bronze.erp_px_cat_g1v2(
    ID NVARCHAR(100),
    CAT NVARCHAR(100),
    SUBCAT NVARCHAR(100),
    MAINTENANCE NVARCHAR(100)
);
GO
