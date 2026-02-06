-- =============================================
-- Exploratory Data Analysis: ERP Customers
-- =============================================

USE DataWarehouse;
GO

-- =============================================
-- Data Profiling Checks
-- =============================================

-- 1. Count total records
SELECT COUNT(*) AS total_bronze_records
FROM bronze.erp_cust_az12;
-- Result: 18483 rows

-- 2. Check for duplicate Customer IDs
SELECT 
    cid,
    COUNT(*) AS duplicate_count
FROM bronze.erp_cust_az12
GROUP BY cid
HAVING COUNT(*) > 1;
-- Finding: 0 duplicates found

-- 3. Check for whitespace issues
SELECT 
    cid,
    gen
FROM bronze.erp_cust_az12
WHERE cid <> TRIM(cid)
   OR gen <> TRIM(gen);
-- Finding: 0 records with leading/trailing whitespace

-- 4. Check for data quality issues (future dates, invalid formats, gender values)
SELECT 
    SUM(CASE WHEN TRY_CAST(bdate AS DATE) > CAST(GETDATE() AS DATE) THEN 1 ELSE 0 END) AS future_birthdates,
    SUM(CASE WHEN TRY_CAST(bdate AS DATE) IS NULL AND bdate IS NOT NULL THEN 1 ELSE 0 END) AS invalid_dates,
    SUM(CASE WHEN cid LIKE 'NAS%' THEN 1 ELSE 0 END) AS nas_prefix_count
FROM bronze.erp_cust_az12;

-- Finding: 16 future birthdates, 0 invalid dates, 11042 records with 'NAS' prefix
-- Decision: Set future birthdates to NULL, remove 'NAS' prefix from customer IDs to enable joining with erp_loc_a101

-- 5. Profile gender values
SELECT DISTINCT gen
FROM bronze.erp_cust_az12;
-- Found: 'F', 'M', 'female', 'male', NULL
-- Decision: Standardize to 'Female', 'Male', 'Unknown' 
