-- =============================================
-- Exploratory Data Analysis: CRM Production Info
-- =============================================

USE DataWarehouse;
GO

-- =============================================
-- Data Profiling Checks
-- =============================================

-- 1. Count total records
SELECT COUNT(*) AS total_product_records
FROM bronze.crm_prd_info;
-- Result: 397 rows

-- 2. Check for duplicate product IDs
SELECT 
    prd_id,
    COUNT(*) AS version_count
FROM bronze.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 
ORDER BY version_count DESC;
-- Result: 0 duplicate product IDs
-- Decision: No deduplication needed

-- 3. Check for NULL product IDs
SELECT COUNT(*) AS null_prd_id_count
FROM bronze.crm_prd_info
WHERE prd_id IS NULL;
-- Result: 0 NULL product IDs

-- 4. Check for whitespace issues
SELECT 
    prd_key,
    prd_nm,
    prd_line
FROM bronze.crm_prd_info
WHERE prd_key <> TRIM(prd_key) 
   OR prd_nm <> TRIM(prd_nm) 
   OR prd_line <> TRIM(prd_line);
-- Result: 0 records with whitespace issues

-- 5. Check cost data quality
SELECT 
    SUM(CASE WHEN prd_cost IS NULL THEN 1 ELSE 0 END) AS null_costs,
    SUM(CASE WHEN TRY_CAST(prd_cost AS DECIMAL(10,2)) IS NULL 
              AND prd_cost IS NOT NULL THEN 1 ELSE 0 END) AS invalid_costs,
    SUM(CASE WHEN TRY_CAST(prd_cost AS DECIMAL(10,2)) < 0 THEN 1 ELSE 0 END) AS negative_costs
FROM bronze.crm_prd_info;
-- Result: 2 NULL costs, 0 invalid formats, 0 negative values
-- Decision: Keep NULL costs to distinguish cost free vs unpriced products

-- 6. Profile product line values
SELECT DISTINCT prd_line
FROM bronze.crm_prd_info;
-- Result: 4 unique values - 'M', 'R', 'S', 'T'
-- Decision: Expand codes (M→Mountain, R→Road, S→Other, T→Touring)
