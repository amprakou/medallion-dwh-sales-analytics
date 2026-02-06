-- =============================================
-- Silver Layer: Master Load Stored Procedure
-- =============================================
-- Purpose: Master procedure to orchestrate Silver layer data loads
-- Strategy: Execute individual table load procedures in sequence
-- Benefits: Modular, maintainable, testable, reusable
-- Run Order: After individual load procedures created (02-07)
--
-- Tables loaded (6 total):
--   - silver.crm_cust_info (CRM customer data)
--   - silver.crm_prd_info (CRM product data)
--   - silver.crm_sales_details (CRM sales transactions)
--   - silver.erp_cust_az12 (ERP customer data)
--   - silver.erp_loc_a101 (ERP location data)
--   - silver.erp_px_cat_g1v2 (ERP product categories)
--
-- Logging: All operations logged to silver.load_log with millisecond precision
-- Usage: EXEC silver.sp_load_silver_master;
-- =============================================

USE DataWarehouse;
GO

-- =============================================
-- Step 1: Create Load Procedure
-- =============================================

DROP PROCEDURE IF EXISTS silver.sp_load_silver_master;
GO

CREATE PROCEDURE silver.sp_load_silver_master
AS
BEGIN
    SET NOCOUNT ON;

    PRINT REPLICATE('=', 50);
    PRINT 'SILVER LAYER MASTER LOAD STARTED';
    PRINT REPLICATE('=', 50);
    PRINT '';

    PRINT 'Loading CRM Customer Info...';
    EXEC silver.sp_load_crm_customer;
    PRINT REPLICATE('-', 50);

    PRINT 'Loading CRM Product Info...';
    EXEC silver.sp_load_crm_product;
    PRINT REPLICATE('-', 50);

    PRINT 'Loading CRM Sales Details...';
    EXEC silver.sp_load_crm_sales;
    PRINT REPLICATE('-', 50);

    PRINT 'Loading ERP Customer (AZ12)...';
    EXEC silver.sp_load_erp_customer;
    PRINT REPLICATE('-', 50);

    PRINT 'Loading ERP Location (A101)...';
    EXEC silver.sp_load_erp_location;
    PRINT REPLICATE('-', 50);

    PRINT 'Loading ERP Product Category (G1V2)...';
    EXEC silver.sp_load_erp_product_category;
    PRINT REPLICATE('-', 50);

    PRINT '';
    PRINT REPLICATE('=', 50);
    PRINT 'SILVER LAYER MASTER LOAD COMPLETED';
    PRINT REPLICATE('=', 50);
END;
GO

-- =============================================
-- Step 2: Execute Procedure
-- =============================================

EXEC silver.sp_load_silver_master;
GO

-- =============================================
-- Step 3: View load history
-- =============================================

SELECT 
    load_id,
    table_name,
    operation_type,
    load_start,
    load_end,
    duration_ms,
    status,
    rows_affected,
    error_message,
    error_number,
    error_severity
FROM silver.load_log
ORDER BY load_start DESC;
GO

/*
Expected Output:

==================================================
SILVER LAYER MASTER LOAD STARTED
==================================================
 
Loading CRM Customer Info...
silver.crm_cust_info: Truncated 18484 rows
silver.crm_cust_info: Inserted 18484 rows
--------------------------------------------------
Loading CRM Product Info...
silver.crm_prd_info: Truncated 397 rows
silver.crm_prd_info: Inserted 397 rows
--------------------------------------------------
Loading CRM Sales Details...
silver.crm_sales_details: Truncated 60398 rows
silver.crm_sales_details: Inserted 60398 rows
--------------------------------------------------
Loading ERP Customer (AZ12)...
silver.erp_cust_az12: Truncated 18483 rows
silver.erp_cust_az12: Inserted 18483 rows
--------------------------------------------------
Loading ERP Location (A101)...
silver.erp_loc_a101: Truncated 18484 rows
silver.erp_loc_a101: Inserted 18484 rows
--------------------------------------------------
Loading ERP Product Category (G1V2)...
silver.erp_px_cat_g1v2: Truncated 37 rows
silver.erp_px_cat_g1v2: Inserted 37 rows
--------------------------------------------------
 
==================================================
SILVER LAYER MASTER LOAD COMPLETED
==================================================
Total execution time: 00:00:00.642

Sample Output : 

load_id  table_name                   operation_type  load_start                  load_end                    duration_ms  status     rows_affected  error_message  error_number  error_severity
-------  ---------------------------  --------------  --------------------------  --------------------------  -----------  ---------  -------------  -------------  ------------  --------------
23       silver.erp_px_cat_g1v2       TRUNCATE        2026-01-28 16:53:39.9066667  2026-01-28 16:53:39.9066667  0            Succeeded  37             NULL           NULL          NULL
24       silver.erp_px_cat_g1v2       INSERT          2026-01-28 16:53:39.9066667  2026-01-28 16:53:39.9066667  0            Succeeded  37             NULL           NULL          NULL
22       silver.erp_loc_a101          INSERT          2026-01-28 16:53:39.8466667  2026-01-28 16:53:39.9033333  57           Succeeded  18484          NULL           NULL          NULL
21       silver.erp_loc_a101          TRUNCATE        2026-01-28 16:53:39.8433333  2026-01-28 16:53:39.8433333  0            Succeeded  18484          NULL           NULL          NULL
20       silver.erp_cust_az12         INSERT          2026-01-28 16:53:39.7933333  2026-01-28 16:53:39.8400000  47           Succeeded  18483          NULL           NULL          NULL
19       silver.erp_cust_az12         TRUNCATE        2026-01-28 16:53:39.7900000  2026-01-28 16:53:39.7933333  3            Succeeded  18483          NULL           NULL          NULL
18       silver.crm_sales_details     INSERT          2026-01-28 16:53:39.4400000  2026-01-28 16:53:39.7866667  346          Succeeded  60398          NULL           NULL          NULL
17       silver.crm_sales_details     TRUNCATE        2026-01-28 16:53:39.4333333  2026-01-28 16:53:39.4400000  7            Succeeded  60398          NULL           NULL          NULL
15       silver.crm_prd_info          TRUNCATE        2026-01-28 16:53:39.4133333  2026-01-28 16:53:39.4133333  0            Succeeded  397            NULL           NULL          NULL
16       silver.crm_prd_info          INSERT          2026-01-28 16:53:39.4133333  2026-01-28 16:53:39.4266667  13           Succeeded  397            NULL           NULL          NULL
14       silver.crm_cust_info         INSERT          2026-01-28 16:53:39.3366667  2026-01-28 16:53:39.4066667  70           Succeeded  18484          NULL           NULL          NULL
13       silver.crm_cust_info         TRUNCATE        2026-01-28 16:53:39.3300000  2026-01-28 16:53:39.3366667  6            Succeeded  18484          NULL           NULL          NULL

*/
