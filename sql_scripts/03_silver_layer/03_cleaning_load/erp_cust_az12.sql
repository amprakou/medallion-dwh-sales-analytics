-- =============================================
-- Silver Layer: Load ERP Customer (AZ12)
-- =============================================
-- Transformations Applied:
-- 1. Key parsing - Remove 'NAS' prefix from customer IDs
        -- Example: 'NASAW00011000' → 'AW00011000'
        -- Purpose: Enable joining with erp_loc_a101 (which has 'AW-00011000')
-- 2. Type conversions - NVARCHAR → DATE
-- 3. Date validation - Set future birthdates to NULL
-- 4. Standardization - Expand gender codes (f/female→Female, m/male→Male)
-- =============================================

USE DataWarehouse;
GO

-- =============================================
-- Insert into Silver Layer
-- =============================================

INSERT INTO silver.erp_cust_az12 (
    CID,
    BDATE,
    GEN
)

-- =============================================
-- Data Cleaning & Transformation
-- =============================================


SELECT
    CASE
        WHEN CID LIKE 'NAS%' THEN SUBSTRING(CID, 4, LEN(CID))
        ELSE CID
    END AS CID,
    
    CASE
        WHEN TRY_CAST(BDATE AS DATE) > CAST(GETDATE() AS DATE) THEN NULL
        ELSE TRY_CAST(BDATE AS DATE)
    END AS BDATE,
    
    CASE
        WHEN LOWER(GEN) IN ('f', 'female') THEN 'Female'
        WHEN LOWER(GEN) IN ('m', 'male') THEN 'Male'
        ELSE 'Unknown'
    END AS GEN
FROM bronze.erp_cust_az12;
GO
