-- =============================================
-- Exploratory Data Analysis : CRM Customer Info
-- =============================================

USE DataWarehouse;
GO

-- =============================================
-- Data Profiling Checks
-- =============================================

-- 1. Count total records
SELECT COUNT(*) AS total_bronze_records
FROM bronze.crm_cust_info;
-- Finding: 18,493 rows

-- 2. Check for duplicate customer IDs
SELECT 
    cst_id,
    COUNT(*) AS duplicate_count
FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1;
-- Finding: 5 customer IDs have duplicates
-- Decision: Keep most recent record based on cst_create_date

-- 3. Examine duplicate records in detail
SELECT * 
FROM bronze.crm_cust_info
WHERE cst_id IN ('29449', '29473', '29433', '29483', '29466') 
   OR cst_id IS NULL
ORDER BY cst_id, cst_create_date DESC;
-- Finding: Duplicate IDs with most recent create_date contain latest customer info
-- Decision: Use ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC)

-- 4. Profile marital status values
SELECT DISTINCT cst_marital_status
FROM bronze.crm_cust_info;
-- Finding: 3 unique values - 'M', 'S', NULL
-- Decision: Expand codes (M→Married, S→Single, NULL→Unknown)

-- 5. Profile gender values
SELECT DISTINCT cst_gndr
FROM bronze.crm_cust_info;
-- Finding: 3 unique values - 'F', 'M', NULL
-- Decision: Expand codes (M→Male, F→Female, NULL→Unknown)

-- 6. Validate date formats
SELECT 
    cst_id,
    cst_create_date,
    TRY_CAST(cst_create_date AS DATE) AS parsed_date
FROM bronze.crm_cust_info
WHERE TRY_CAST(cst_create_date AS DATE) IS NULL;
-- Finding: NULL dates exist when cst_id is also NULL
-- Decision: These records will be filtered out (NULL customer IDs are invalid)

-- 7. Check for trailing/leading whitespace
SELECT 
    cst_firstname,
    cst_lastname
FROM bronze.crm_cust_info
WHERE cst_firstname <> TRIM(cst_firstname) 
   OR cst_lastname <> TRIM(cst_lastname);
-- Finding: Whitespace detected in names
-- Decision: Apply TRIM() in Silver layer transformation
