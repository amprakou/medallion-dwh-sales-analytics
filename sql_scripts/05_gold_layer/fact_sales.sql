-- =============================================
-- Gold Layer: Sales Fact Table
-- =============================================
-- Purpose: Sales transactions fact table
-- One row per product per sales order
-- Sources: silver.crm_sales_details, gold.dim_products, gold.dim_customers
-- Foreign Keys: customer_key, product_key, order_date_key (to be joined with dim_date)
-- =============================================

USE DataWarehouse;
GO


DROP VIEW IF EXISTS gold.fact_sales;
GO

CREATE VIEW gold.fact_sales AS
SELECT
    sd.sls_ord_num AS order_number,
    pr.product_key AS product_key,
    cu.customer_key AS customer_key,
    sd.sls_order_dt AS order_date,
    CAST(FORMAT(sd.sls_order_dt, 'yyyyMMdd') AS INT) AS order_date_key,
    sd.sls_ship_dt AS shipping_date,
    sd.sls_due_dt AS due_date,
    sd.sls_sales AS sales_amount,
    sd.sls_quantity AS quantity,
    sd.sls_price AS unit_price
FROM silver.crm_sales_details sd
LEFT JOIN gold.dim_products pr ON sd.sls_prd_key = pr.product_number
LEFT JOIN gold.dim_customers cu ON sd.sls_cust_id = cu.customer_id;
GO
