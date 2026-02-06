-- =============================================
-- Exploratory Data Analysis: ERP Locations
-- =============================================

USE DataWarehouse;
GO

-- =============================================
-- Data Profiling Checks
-- =============================================

-- 1. Count total records
SELECT COUNT(*) AS total_bronze_records
FROM bronze.erp_loc_a101;
-- Result: 18,484 rows

-- 2. Check for duplicate Customer IDs
SELECT 
    CID,
    COUNT(*) AS duplicate_count
FROM bronze.erp_loc_a101
GROUP BY CID
HAVING COUNT(*) > 1;
-- Finding: 0 duplicates found

-- 3. Check for NULL or wrong prefix Customer IDs
SELECT 
    COUNT(CID) AS invalid_customer_ids
FROM bronze.erp_loc_a101
WHERE CID IS NULL OR CID LIKE 'NAS%';
-- Finding: 0 NULL customer IDs or IDs with wrong prefix

-- 4. Profile country values
SELECT DISTINCT CNTRY
FROM bronze.erp_loc_a101
ORDER BY CNTRY;
-- Found: 'US', 'USA', 'United States', 'DE', 'Germany', NULL, ''
-- Decision: Normalize to 'United States', 'Germany', 'Unknown'
