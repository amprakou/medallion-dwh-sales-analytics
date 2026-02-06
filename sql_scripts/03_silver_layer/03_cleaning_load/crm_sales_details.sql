-- =============================================
-- Silver Layer: Load CRM Sales Details
-- =============================================
-- Transformations Applied:
-- 1. Type conversions - NVARCHAR → INT, DECIMAL, DATE
-- 2. Date validation - Filter year outliers (< 1990 or > current year + 1)
-- 2a. Impute sls_order_dt USING DATEADD(DAY, -7, sls_ship_dt)
-- 3. Business logic - Recalculate sales as quantity × price when inconsistent
-- 4. Business logic - Derive price from sales ÷ quantity when missing
-- 5. Data validation - Ensure amounts > 0
-- =============================================

USE DataWarehouse;
GO

-- =============================================
-- Insert into Silver Layer
-- =============================================

INSERT INTO silver.crm_sales_details (
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    sls_order_dt,
    sls_ship_dt,
    sls_due_dt,
    sls_sales,
    sls_quantity,
    sls_price
)

-- =============================================
-- Data Cleaning & Transformation
-- =============================================
    
SELECT 
    sls_ord_num,
    sls_prd_key,
    TRY_CAST(sls_cust_id AS INT) AS cust_id,
    
    CASE 
     
        WHEN a.sls_order_dt IS NOT NULL
          AND YEAR(a.sls_order_dt) >= 1990
           AND YEAR(a.sls_order_dt) <= YEAR(GETDATE()) + 1
        THEN a.sls_order_dt
          
        WHEN (a.sls_order_dt IS NULL 
          OR YEAR(a.sls_order_dt) < 1990
           OR YEAR(a.sls_order_dt) > YEAR(GETDATE()) + 1)
           AND a.sls_ship_dt IS NOT NULL
           AND YEAR(a.sls_ship_dt) >= 1990
           AND YEAR(a.sls_ship_dt) <= YEAR(GETDATE()) + 1
        THEN DATEADD(DAY, -7, a.sls_ship_dt)     
        ELSE NULL
    END AS sls_order_dt,
    
    CASE 
        WHEN a.sls_ship_dt IS NOT NULL 
          AND (YEAR(a.sls_ship_dt) > YEAR(GETDATE()) +1
           OR YEAR(a.sls_ship_dt) < 1990)
        THEN NULL 
        ELSE a.sls_ship_dt 
    END AS sls_ship_dt,
    
    CASE 
        WHEN a.sls_due_dt IS NOT NULL 
          AND (YEAR(a.sls_due_dt) > YEAR(GETDATE()) +1 
           OR YEAR(a.sls_due_dt) < 1990)
        THEN NULL 
        ELSE a.sls_due_dt 
    END AS sls_due_dt,
    
    CASE 
        WHEN b.sls_sales IS NULL 
          OR b.sls_sales <= 0 
          OR b.sls_sales <> (b.sls_quantity * b.sls_price)
        THEN b.sls_quantity * b.sls_price
        ELSE b.sls_sales
    END AS sls_sales,

    b.sls_quantity as sls_quantity,
    
    CASE 
        WHEN b.sls_price IS NULL OR b.sls_price <= 0 
        THEN TRY_CAST(b.sls_sales / NULLIF(b.sls_quantity, 0) AS DECIMAL(18,2))
        ELSE b.sls_price
    END AS sls_price
    
    
FROM bronze.crm_sales_details

CROSS APPLY (
    SELECT
        TRY_CAST(sls_order_dt AS DATE) AS sls_order_dt,
        TRY_CAST(sls_ship_dt AS DATE) AS sls_ship_dt,
        TRY_CAST(sls_due_dt AS DATE) AS sls_due_dt
) a

CROSS APPLY (
    SELECT
        TRY_CAST(sls_sales AS DECIMAL(18,2)) AS sls_sales,
        TRY_CAST(sls_quantity AS INT) AS sls_quantity,
        TRY_CAST(sls_price AS DECIMAL(18,2)) AS sls_price
) b;
