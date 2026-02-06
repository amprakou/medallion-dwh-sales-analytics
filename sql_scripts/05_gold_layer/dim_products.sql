-- =============================================
-- Gold Layer: Product Dimension
-- =============================================
-- Purpose: Product master dimension
-- Grain: One row per current product version
-- Sources: silver.crm_prd_info, silver.erp_px_cat_g1v2
-- Business Rule: Only current versions included (prd_end_dt IS NULL)
-- =============================================

USE DataWarehouse;
GO

DROP VIEW IF EXISTS gold.dim_products;
GO

CREATE VIEW gold.dim_products AS
SELECT
    ROW_NUMBER() OVER (ORDER BY pri.prd_start_dt, pri.prd_key) AS product_key,
    pri.prd_id AS product_id,
    pri.prd_key AS product_number,
    pri.prd_nm AS product_name,
    pri.cat_id AS category_id,
    cat.CAT AS category,
    cat.SUBCAT AS subcategory,
    cat.MAINTENANCE AS maintenance,
    pri.prd_cost AS cost,
    pri.prd_line AS product_line,
    pri.prd_start_dt AS start_date
FROM silver.crm_prd_info pri
LEFT JOIN silver.erp_px_cat_g1v2 cat ON pri.cat_id = cat.ID
WHERE pri.prd_end_dt IS NULL;
GO
