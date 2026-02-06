-- =============================================
-- Silver Layer: Load CRM Customer Info
-- =============================================

-- Transformations Applied:
-- 1. Deduplication - Keep most recent record per customer
-- 2. Type conversions - NVARCHAR → INT, DATE, VARCHAR
-- 3. Data cleaning - TRIM whitespace from names
-- 4. Standardization (M→Married/Male, S→Single ,F→Female, NULL→Unknown )
-- 5. NULL handling - Exclude records with NULL customer_id
-- =============================================


-- =============================================
-- Data Cleaning & Transformation
-- =============================================


WITH cleaning_cust_info AS (
    SELECT 
        TRY_CAST(cst_id AS INT) AS cst_id,
        ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY TRY_CAST(cst_create_date AS DATE) DESC) AS rn,
        TRY_CAST(cst_key AS VARCHAR(15)) AS cst_key,
        TRIM(cst_firstname) AS cst_firstname,
        TRIM(cst_lastname) AS cst_lastname,

        CASE 
            WHEN TRIM(cst_marital_status) = 'M' THEN 'Married'
            WHEN TRIM(cst_marital_status) = 'S' THEN 'Single'
            ELSE 'Unknown'
        END AS cst_marital_status,

        CASE 
            WHEN TRIM(cst_gndr) = 'M' THEN 'Male'
            WHEN TRIM(cst_gndr) = 'F' THEN 'Female'
            ELSE 'Unknown'
        END AS cst_gndr,
  
        TRY_CAST(cst_create_date AS DATE) AS cst_create_date
    FROM bronze.crm_cust_info
)

-- =============================================
-- Insert into Silver Layer
-- =============================================
    
    
INSERT INTO silver.crm_cust_info (
    cst_id,
    cst_key,
    cst_firstname,
    cst_lastname,
    cst_marital_status,
    cst_gndr,
    cst_create_date
)
SELECT 
    cst_id,
    cst_key,
    cst_firstname,
    cst_lastname,
    cst_marital_status,
    cst_gndr,
    cst_create_date
FROM cleaning_cust_info
WHERE rn = 1              -- keeping most recent record
  AND cst_id IS NOT NULL; -- exclude invalid customer id's
