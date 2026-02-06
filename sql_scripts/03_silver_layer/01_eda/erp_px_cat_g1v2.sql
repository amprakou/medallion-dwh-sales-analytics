-- =============================================
-- Exploratory Data Analysis: ERP Categories
-- =============================================

USE DataWarehouse;
GO

-- =============================================
-- Data Profiling Checks
-- =============================================

-- 1. Count total records
SELECT COUNT(*) AS total_catgory_records
FROM bronze.erp_px_cat_g1v2
-- Result: 37 rows

-- 2. Check for duplicate Product IDs
SELECT 
    ID,
    COUNT(*) AS duplicate_count
FROM bronze.erp_px_cat_g1v2
GROUP BY ID
HAVING COUNT(*) > 1;
-- Finding: 0 duplicates found

-- 3. Check for NULL Product IDs
SELECT 
    COUNT(*) AS null_product_ids
FROM bronze.erp_px_cat_g1v2
WHERE ID IS NULL;
-- Finding: 0 NULL product IDs

-- 4. Check for whitespace issues
SELECT 
    CAT,
    SUBCAT,
    MAINTENANCE
FROM bronze.erp_px_cat_g1v2
WHERE CAT <> TRIM(CAT)
   OR SUBCAT <> TRIM(SUBCAT)
   OR MAINTENANCE <> TRIM(MAINTENANCE);
-- Finding: 0 records with leading/trailing whitespace
-- Decision : All data quality checks passed - no cleaning or transformation needed
