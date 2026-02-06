-- =============================================
-- Silver Layer Table Creation
-- =============================================
-- Purpose: Create cleaned, business-ready tables for analytics
-- Layer: Silver (Clean, Conformed)
-- Dependencies: Bronze layer tables must exist
-- =============================================

USE DataWarehouse;
GO

-- =============================================
-- CRM Tables
-- =============================================

-- 1. CRM Customer Info
DROP TABLE IF EXISTS silver.crm_cust_info;
GO

CREATE TABLE silver.crm_cust_info (
    cst_id INT NOT NULL,
    cst_key VARCHAR(15),
    cst_firstname NVARCHAR(100),
    cst_lastname NVARCHAR(100),
    cst_marital_status NVARCHAR(50),
    cst_gndr NVARCHAR(50),
    cst_create_date DATE,
    loaded_at DATETIME2  DEFAULT GETDATE()
);
GO


-- 2. CRM Product Info (SCD Type 2)
DROP TABLE IF EXISTS silver.crm_prd_info;
GO

CREATE TABLE silver.crm_prd_info (
    prd_id INT NOT NULL,
    cat_id VARCHAR(10),
    prd_key VARCHAR(15) NOT NULL,
    prd_nm NVARCHAR(100),
    prd_cost DECIMAL(10,2),
    prd_line NVARCHAR(50),
    prd_start_dt DATE NOT NULL,
    prd_end_dt DATE,
    loaded_at DATETIME2  DEFAULT GETDATE()
);
GO

    
-- 3. CRM Sales Details
DROP TABLE IF EXISTS silver.crm_sales_details;
GO

CREATE TABLE silver.crm_sales_details (
    sls_ord_num NVARCHAR(50) NOT NULL,
    sls_prd_key VARCHAR(15) NOT NULL,
    sls_cust_id INT NOT NULL,
    sls_order_dt DATE NOT NULL,
    sls_ship_dt DATE,
    sls_due_dt DATE,
    sls_sales DECIMAL(18,2) NOT NULL,
    sls_quantity INT NOT NULL,
    sls_price DECIMAL(18,2) NOT NULL,
    loaded_at DATETIME2  DEFAULT GETDATE()
);
GO

-- 4. ERP Customer (AZ12)
DROP TABLE IF EXISTS silver.erp_cust_az12;
GO

CREATE TABLE silver.erp_cust_az12 (
    CID NVARCHAR(50) NOT NULL,
    BDATE DATE,
    GEN NVARCHAR(50),
    loaded_at DATETIME2  DEFAULT GETDATE()
);
GO

-- 5. ERP Location (A101)

DROP TABLE IF EXISTS silver.erp_loc_a101;
GO

CREATE TABLE silver.erp_loc_a101 (
    CID NVARCHAR(50) NOT NULL,
    CNTRY NVARCHAR(100),
    loaded_at DATETIME2  DEFAULT GETDATE()
);
GO

-- 6. ERP Product Category (G1V2)
DROP TABLE IF EXISTS silver.erp_px_cat_g1v2;
GO

CREATE TABLE silver.erp_px_cat_g1v2 (
    ID NVARCHAR(50) NOT NULL,
    CAT NVARCHAR(100),
    SUBCAT NVARCHAR(100),
    MAINTENANCE CHAR(5),
    loaded_at DATETIME2 DEFAULT GETDATE()
);
GO
