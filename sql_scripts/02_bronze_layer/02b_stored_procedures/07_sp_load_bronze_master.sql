-- =============================================
-- Bronze Layer: Master Load Stored Procedure
-- =============================================
-- Purpose: Master procedure to orchestrate Bronze layer data loads
-- Strategy: Execute individual table load procedures in sequence
-- Benefits: Modular, maintainable, testable, reusable
-- Run Order: After individual load procedures created
--
-- Tables loaded (6 total):
--   - bronze.crm_cust_info (CRM customer data)
--   - bronze.crm_prd_info (CRM product data)
--   - bronze.crm_sales_details (CRM sales transactions)
--   - bronze.erp_cust_az12 (ERP customer data)
--   - bronze.erp_loc_a101 (ERP location data)
--   - bronze.erp_px_cat_g1v2 (ERP product categories)
--
-- Logging: All operations logged to bronze.load_log with millisecond precision
-- Usage: EXEC bronze.sp_load_bronze_master;
-- =============================================


CREATE PROCEDURE bronze.sp_load_bronze_master
AS
BEGIN
    SET NOCOUNT ON;

        PRINT REPLICATE('=', 50);
        PRINT 'BRONZE MASTER LOAD STARTED';
        PRINT REPLICATE('=', 50);
        
        PRINT 'Loading CRM Customer Info...';
        EXEC bronze.load_crm_cust_info;
        PRINT REPLICATE('-', 50);
        
        PRINT 'Loading CRM Product Info...';
        EXEC bronze.load_crm_prd_info;
        PRINT REPLICATE('-', 50);
        
        PRINT 'Loading CRM Sales Details...';
        EXEC bronze.load_crm_sales_details;
        PRINT REPLICATE('-', 50);
        
        PRINT 'Loading ERP Customer (AZ12)...';
        EXEC bronze.load_erp_cust_az12;
        PRINT REPLICATE('-', 50);
        
        PRINT 'Loading ERP Location (A101)...';
        EXEC bronze.load_erp_loc_a101;
        PRINT REPLICATE('-', 50);
        
        PRINT 'Loading ERP Product Category (G1V2)...';
        EXEC bronze.load_erp_px_cat_g1v2;
        PRINT REPLICATE('-', 50);
        
        PRINT REPLICATE('=', 50);
        PRINT 'BRONZE MASTER LOAD COMPLETED';
        PRINT REPLICATE('=', 50);

END;
GO

-- =============================================
-- Step 2: Execute Procedure
-- =============================================

EXEC bronze.sp_load_bronze_master;

-- =============================================
-- Step 3: View load history
-- =============================================

SELECT 
  load_id,
  table_name,
  type,
  load_start,
  load_end,
  duration_ms,
  status,
  rows_affected,
  error_message,
  error_number,
  ERROR_SEVERITY
  FROM bronze.load_log ;

/*
Sample Output : 

                load_id  table_name                   type      load_start                  load_end                    duration_ms  status     rows_affected  error_message  error_number  ERROR_SEVERITY
                -------  ---------------------------  --------  --------------------------  --------------------------  -----------  ---------  -------------  -------------  ------------  --------------
                1        bronze.crm_cust_info          TRUNCATE  2026-01-27 16:34:33.3300000  2026-01-27 16:34:33.3333333  3            Succeeded  18493          NULL           NULL          NULL
                2        bronze.crm_cust_info          INSERT    2026-01-27 16:34:33.3333333  2026-01-27 16:34:33.3700000  37           Succeeded  18493          NULL           NULL          NULL
                3        bronze.crm_prd_info           TRUNCATE  2026-01-27 16:34:33.3700000  2026-01-27 16:34:33.3700000  0            Succeeded  397            NULL           NULL          NULL
                4        bronze.crm_prd_info           INSERT    2026-01-27 16:34:33.3733333  2026-01-27 16:34:33.3733333  0            Succeeded  397            NULL           NULL          NULL
                5        bronze.crm_sales_details      TRUNCATE  2026-01-27 16:34:33.3766667  2026-01-27 16:34:33.3833333  7            Succeeded  60398          NULL           NULL          NULL
                6        bronze.crm_sales_details      INSERT    2026-01-27 16:34:33.3833333  2026-01-27 16:34:33.4966667  113          Succeeded  60398          NULL           NULL          NULL
                7        bronze.erp_cust_az12          TRUNCATE  2026-01-27 16:34:33.4966667  2026-01-27 16:34:33.4966667  0            Succeeded  18483          NULL           NULL          NULL
                8        bronze.erp_cust_az12          INSERT    2026-01-27 16:34:33.4966667  2026-01-27 16:34:33.5166667  20           Succeeded  18483          NULL           NULL          NULL
                9        bronze.erp_loc_a101           TRUNCATE  2026-01-27 16:34:33.5166667  2026-01-27 16:34:33.5200000  4            Succeeded  18484          NULL           NULL          NULL
                10       bronze.erp_loc_a101           INSERT    2026-01-27 16:34:33.5200000  2026-01-27 16:34:33.5333333  13           Succeeded  18484          NULL           NULL          NULL
                11       bronze.erp_px_cat_g1v2        TRUNCATE  2026-01-27 16:34:33.5333333  2026-01-27 16:34:33.5333333  0            Succeeded  37             NULL           NULL          NULL
                12       bronze.erp_px_cat_g1v2        INSERT    2026-01-27 16:34:33.5333333  2026-01-27 16:34:33.5366667  3            Succeeded  37             NULL           NULL          NULL  
