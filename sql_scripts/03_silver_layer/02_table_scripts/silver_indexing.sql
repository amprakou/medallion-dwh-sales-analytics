-- =============================================
-- Silver Layer: Index Creation
-- =============================================
-- Purpose: Create indexes for query performance on Silver tables
-- Usage: Optimize joins for Gold layer views and ETL operations
-- Run Order: After Silver tables creation and data loading
-- Dependencies: silver_table_creation.sql must be executed first
-- =============================================
USE DataWarehouse;
GO

-- =============================================
-- 1. silver.crm_cust_info
-- =============================================
  
-- Clustered index for primary lookups
CREATE CLUSTERED INDEX IX_crm_cust_info_cst_id 
ON silver.crm_cust_info(cst_id);
GO

-- Index for JOIN with ERP tables (cst_key)
CREATE NONCLUSTERED INDEX IX_crm_cust_info_cst_key 
ON silver.crm_cust_info(cst_key);
GO

-- =============================================
-- 2. silver.crm_prd_info
-- =============================================
  
-- Clustered index for product lookups 
CREATE CLUSTERED INDEX IX_crm_prd_info_key_dates 
ON silver.crm_prd_info(prd_key, prd_start_dt DESC);
GO

-- Filtered index for active products
CREATE NONCLUSTERED INDEX IX_crm_prd_info_active 
ON silver.crm_prd_info(prd_key)
WHERE prd_end_dt IS NULL;
GO

-- =============================================
-- 3. silver.crm_sales_details
-- =============================================
  
-- Clustered index for date-based queries
CREATE CLUSTERED INDEX IX_crm_sales_order_date 
ON silver.crm_sales_details(sls_order_dt, sls_ord_num);
GO

-- Index for JOIN with dim_products
CREATE NONCLUSTERED INDEX IX_crm_sales_prd_key 
ON silver.crm_sales_details(sls_prd_key);
GO

-- Index for JOIN with dim_customers
CREATE NONCLUSTERED INDEX IX_crm_sales_cust_id 
ON silver.crm_sales_details(sls_cust_id);
GO

-- =============================================
-- 4. silver.erp_cust_az12
-- =============================================
  
-- Clustered index for JOIN with crm_cust_info
CREATE CLUSTERED INDEX IX_erp_cust_az12_cid 
ON silver.erp_cust_az12(CID);
GO

-- =============================================
-- 5. silver.erp_loc_a101
-- =============================================
  
-- Clustered index for JOIN with crm_cust_info
CREATE CLUSTERED INDEX IX_erp_loc_a101_cid 
ON silver.erp_loc_a101(CID);
GO

-- =============================================
-- 6. silver.erp_px_cat_g1v2
-- =============================================
  
-- Clustered index for JOIN with crm_prd_info
CREATE CLUSTERED INDEX IX_erp_px_cat_id 
ON silver.erp_px_cat_g1v2(ID);
GO
