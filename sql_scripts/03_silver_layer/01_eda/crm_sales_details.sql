-- =============================================
-- Exploratory Data Analysis: CRM Sales Details
-- =============================================

USE DataWarehouse;
GO

-- =============================================
-- Data Profiling Checks
-- =============================================

-- 1. Count total records
SELECT COUNT(*) AS total_sales_records
FROM bronze.crm_sales_details;
-- Result: 60398 rows

-- 2. Check for NULL critical fields
SELECT 
    SUM(CASE WHEN sls_ord_num IS NULL OR sls_ord_num = '' THEN 1 ELSE 0 END) AS null_order_numbers,
    SUM(CASE WHEN sls_prd_key IS NULL OR sls_prd_key = '' THEN 1 ELSE 0 END) AS null_product_keys,
    SUM(CASE WHEN sls_cust_id IS NULL OR sls_cust_id = '' THEN 1 ELSE 0 END) AS null_customer_ids
FROM bronze.crm_sales_details;
-- Result: 0 records with NULL critical fields
-- Decision: No action needed - all critical fields populated

-- 3. Check for whitespace issues
SELECT 
    sls_ord_num,
    sls_prd_key,
    sls_cust_id
FROM bronze.crm_sales_details
WHERE sls_ord_num <> TRIM(sls_ord_num)
   OR sls_prd_key <> TRIM(sls_prd_key)
   OR sls_cust_id <> TRIM(sls_cust_id);
-- Result: 0 records with leading/trailing whitespace
-- Decision: No TRIM() needed for these fields

-- 4. Check for invalid or missing amounts

SELECT 
    SUM(CASE WHEN sls_sales IS NULL THEN 1 ELSE 0 END) AS null_sales,
    SUM(CASE WHEN TRY_CAST(sls_sales AS DECIMAL(18,2)) IS NULL 
              AND sls_sales IS NOT NULL THEN 1 ELSE 0 END) AS invalid_sales,
    SUM(CASE WHEN TRY_CAST(sls_sales AS DECIMAL(18,2)) <= 0 THEN 1 ELSE 0 END) AS zero_or_negative_sales,
    
    SUM(CASE WHEN sls_price IS NULL THEN 1 ELSE 0 END) AS null_prices,
    SUM(CASE WHEN TRY_CAST(sls_price AS DECIMAL(18,2)) IS NULL 
              AND sls_price IS NOT NULL THEN 1 ELSE 0 END) AS invalid_prices,
    SUM(CASE WHEN TRY_CAST(sls_price AS DECIMAL(18,2)) <= 0 THEN 1 ELSE 0 END) AS zero_or_negative_prices,
    
    SUM(CASE WHEN sls_quantity IS NULL THEN 1 ELSE 0 END) AS null_quantities,
    SUM(CASE WHEN TRY_CAST(sls_quantity AS INT) IS NULL 
              AND sls_quantity IS NOT NULL THEN 1 ELSE 0 END) AS invalid_quantities,
    SUM(CASE WHEN TRY_CAST(sls_quantity AS INT) <= 0 THEN 1 ELSE 0 END) AS zero_or_negative_quantities
FROM bronze.crm_sales_details;
-- Result: 25 records with NULL or invalid amounts
-- Decision: Recalculate sales_amount from quantity × price when inconsistent or missing

-- 5. Check for NULL order dates
-- Objective: Identify records with missing critical dates in sales data.
SELECT *
FROM silver.crm_sales_details
WHERE sls_order_dt IS NULL
   OR sls_ship_dt IS NULL 
   OR sls_due_dt IS NULL;
-- Result: 19 records have NULL sls_order_dt, 0 records have NULL sls_ship_dt, 0 records have NULL sls_due_dt


-- 5a. Check the pattern between order and ship dates
SELECT 
    DATEDIFF(DAY, sls_order_dt, sls_ship_dt) AS dd
FROM silver.crm_sales_details;
-- Finding: All orders are shipped exactly 7 days after the order date.
-- Decision: Impute NULL sls_order_dt values by subtracting 7 days from sls_ship_dt


-- 6. Check for invalid date sequences, unparseable dates, and year outliers
SELECT
    COUNT(*) AS records_with_issues,
    
    -- Date sequence issues
    SUM(CASE WHEN TRY_CAST(sls_order_dt AS DATE) > TRY_CAST(sls_ship_dt AS DATE) THEN 1 ELSE 0 END) AS order_after_ship,
    SUM(CASE WHEN TRY_CAST(sls_ship_dt AS DATE) > TRY_CAST(sls_due_dt AS DATE) THEN 1 ELSE 0 END) AS ship_after_due,
    SUM(CASE WHEN TRY_CAST(sls_order_dt AS DATE) > TRY_CAST(sls_due_dt AS DATE) THEN 1 ELSE 0 END) AS order_after_due,
    
    -- Unparseable dates
    SUM(CASE WHEN TRY_CAST(sls_order_dt AS DATE) IS NULL AND sls_order_dt IS NOT NULL THEN 1 ELSE 0 END) AS invalid_order_date,
    SUM(CASE WHEN TRY_CAST(sls_ship_dt AS DATE) IS NULL AND sls_ship_dt IS NOT NULL THEN 1 ELSE 0 END) AS invalid_ship_date,
    SUM(CASE WHEN TRY_CAST(sls_due_dt AS DATE) IS NULL AND sls_due_dt IS NOT NULL THEN 1 ELSE 0 END) AS invalid_due_date,
    
    -- Year outliers (< 1990 or > current year)
    SUM(CASE WHEN YEAR(TRY_CAST(sls_order_dt AS DATE)) < 1990 
              OR YEAR(TRY_CAST(sls_order_dt AS DATE)) > YEAR(GETDATE()) +1 THEN 1 ELSE 0 END) AS order_year_outlier,
    SUM(CASE WHEN YEAR(TRY_CAST(sls_ship_dt AS DATE)) < 1990 
              OR YEAR(TRY_CAST(sls_ship_dt AS DATE)) > YEAR(GETDATE()) +1 THEN 1 ELSE 0 END) AS ship_year_outlier,
    SUM(CASE WHEN YEAR(TRY_CAST(sls_due_dt AS DATE)) < 1990 
              OR YEAR(TRY_CAST(sls_due_dt AS DATE)) > YEAR(GETDATE()) +1 THEN 1 ELSE 0 END) AS due_year_outlier
FROM bronze.crm_sales_details
WHERE TRY_CAST(sls_order_dt AS DATE) > TRY_CAST(sls_ship_dt AS DATE)
   OR TRY_CAST(sls_ship_dt AS DATE) > TRY_CAST(sls_due_dt AS DATE)
   OR TRY_CAST(sls_order_dt AS DATE) > TRY_CAST(sls_due_dt AS DATE)
   OR (TRY_CAST(sls_order_dt AS DATE) IS NULL AND sls_order_dt IS NOT NULL)
   OR (TRY_CAST(sls_ship_dt AS DATE) IS NULL AND sls_ship_dt IS NOT NULL)
   OR (TRY_CAST(sls_due_dt AS DATE) IS NULL AND sls_due_dt IS NOT NULL)
   OR YEAR(TRY_CAST(sls_order_dt AS DATE)) < 1990
   OR YEAR(TRY_CAST(sls_order_dt AS DATE)) > YEAR(GETDATE()) +1
   OR YEAR(TRY_CAST(sls_ship_dt AS DATE)) < 1990
   OR YEAR(TRY_CAST(sls_ship_dt AS DATE)) > YEAR(GETDATE()) +1
   OR YEAR(TRY_CAST(sls_due_dt AS DATE)) < 1990
   OR YEAR(TRY_CAST(sls_due_dt AS DATE)) > YEAR(GETDATE()) +1;
-- Result: 19 records with date quality issues (21 total issues - some records have multiple problems)
-- Decision: Filter year outliers (< 1990 or > current year) and set to NULL; validate date sequences in Silver layer
