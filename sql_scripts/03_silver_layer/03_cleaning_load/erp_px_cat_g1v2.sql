-- =============================================
-- Silver Layer: Load ERP Product Category (G1V2)
-- =============================================
-- Transformations Applied:
-- None - Clean dataset, direct transfer from Bronze to Silver
-- =============================================

USE DataWarehouse;
GO

-- =============================================
-- Insert into Silver Layer
-- =============================================

INSERT INTO silver.erp_px_cat_g1v2 (
    ID,
    CAT,
    SUBCAT,
    MAINTENANCE
)
SELECT
    ID,
    CAT,
    SUBCAT,
    MAINTENANCE
FROM bronze.erp_px_cat_g1v2;
GO
