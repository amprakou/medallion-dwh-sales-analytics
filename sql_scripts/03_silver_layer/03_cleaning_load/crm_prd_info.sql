-- =============================================
-- Silver Layer: Load CRM Product Info
-- =============================================
-- Transformations Applied:
-- 1. Type conversions - NVARCHAR → INT, DECIMAL, DATE
-- 2. Key parsing - Split prd_key into cat_id and prd_key
        -- Example: 'AC-BR_BK-R93R-58' → cat_id: 'AC_BR', prd_key: 'BK-R93R-58'
        -- Purpose: Enable joining with erp_px_cat_g1v2 on cat_id (ID column)
-- 3. Standardization - Expand product line codes (M→Mountain, R→Road, etc.)
-- 4. Calculate end dates using LEAD() :
        -- PARTITION BY parsed_prd_key groups all versions of same product
        -- ORDER BY prd_start_dt sorts versions chronologically
        -- LEAD(prd_start_dt) gets NEXT version's start date
        -- DATEADD(DAY, -1, ...) subtracts 1 day for end date
-- 5. NULL handling - Exclude records with NULL product IDs
-- =============================================

USE DataWarehouse;
GO

-- =============================================
-- Data Cleaning & Transformation (CTE)
-- =============================================

WITH cleaning_products AS (
    SELECT 
        TRY_CAST(prd_id AS INT) AS prd_id,
        REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
        SUBSTRING(prd_key, 7, LEN(prd_key)) AS parsed_prd_key,
        prd_nm,
        TRY_CAST(prd_cost AS DECIMAL(10,2)) AS prd_cost,
        CASE 
            WHEN prd_line = 'M' THEN 'Mountain'
            WHEN prd_line = 'R' THEN 'Road'
            WHEN prd_line = 'S' THEN 'Other'
            WHEN prd_line = 'T' THEN 'Touring'
            ELSE 'Unknown'
        END AS prd_line,
        TRY_CAST(prd_start_dt AS DATE) AS prd_start_dt
    FROM bronze.crm_prd_info
    WHERE TRY_CAST(prd_id AS INT) IS NOT NULL
)

-- =============================================
-- Insert into Silver Layer
-- =============================================
  
INSERT INTO silver.crm_prd_info (
    prd_id,
    cat_id,
    prd_key,
    prd_nm,
    prd_cost,
    prd_line,
    prd_start_dt,
    prd_end_dt
)
SELECT 
    prd_id,
    cat_id,
    parsed_prd_key AS prd_key,
    prd_nm,
    prd_cost,
    prd_line,
    prd_start_dt,
    DATEADD(
        DAY, -1,
            LEAD(prd_start_dt) OVER (PARTITION BY parsed_prd_key ORDER BY prd_start_dt)) AS prd_end_dt
FROM cleaning_products;
GO
