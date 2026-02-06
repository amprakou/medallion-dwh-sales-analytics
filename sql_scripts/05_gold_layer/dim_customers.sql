-- =============================================
-- Gold Layer: Customer Dimension
-- =============================================
-- Purpose: Customer master dimension combining CRM and ERP data
-- One row per unique customer
-- Sources: silver.crm_cust_info, silver.erp_cust_az12, silver.erp_loc_a101
-- Business Rule: Gender priority - CRM primary, ERP fallback for 'Unknown'
-- =============================================

USE DataWarehouse;
GO

DROP VIEW IF EXISTS gold.dim_customers
GO

CREATE VIEW gold.dim_customers AS
SELECT 
    ROW_NUMBER() OVER (ORDER BY cst_id) AS customer_key,
    cst_id AS customer_id,
    cst_key AS customer_number,
    cst_firstname AS first_name,
    cst_lastname AS last_name,
    CASE 
        WHEN cst_gndr <> 'Unknown' THEN cst_gndr
        ELSE COALESCE(erpc.gen, 'Unknown')
    END AS gender,
    cst_marital_status AS marital_status,
    erpc.BDATE AS birth_date,
    erpl.CNTRY AS country,
    cst_create_date AS create_date
FROM silver.crm_cust_info crmc
LEFT JOIN silver.erp_cust_az12 erpc ON erpc.CID = crmc.cst_key
LEFT JOIN silver.erp_loc_a101 erpl ON crmc.cst_key = erpl.CID;
GO
