-- =============================================
-- Silver Layer: Load ERP Location (A101)
-- =============================================
-- Transformations Applied:
-- 1. Key parsing - Remove '-' delimiter from customer IDs
            -- Example: 'AW-00011000' → 'AW00011000'
            -- Purpose: Enable joining with erp_cust_az12 (which has 'NASAW00011000' → 'AW00011000')
-- 2. Standardization - Normalize country codes (DE→Germany, US/USA→United States)
-- 3. NULL handling - Set empty/NULL countries to 'Unknown'
-- =============================================

USE DataWarehouse;
GO

-- =============================================
-- Insert into Silver Layer
-- =============================================

INSERT INTO silver.erp_loc_a101 (
    CID,
    CNTRY
)

-- =============================================
-- Data Cleaning & Transformation
-- =============================================

SELECT
    REPLACE(CID, '-', '') AS CID,
    
    CASE
        WHEN CNTRY = 'DE' THEN 'Germany'
        WHEN CNTRY IN ('US', 'USA', 'United States') THEN 'United States'
        WHEN CNTRY = '' OR CNTRY IS NULL THEN 'Unknown'
        ELSE CNTRY
    END AS CNTRY
FROM bronze.erp_loc_a101;
GO
