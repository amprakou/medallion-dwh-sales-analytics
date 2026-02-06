-- =============================================
-- Silver Layer: Automated Load Procedure - CRM Sales
-- =============================================
-- Purpose: Load cleaned sales transaction data from Bronze to Silver layer
-- Strategy: Full reload - TRUNCATE + INSERT with business logic validation
-- Source: bronze.crm_sales_details
-- Target: silver.crm_sales_details
-- Logging: All operations logged to silver.load_log
-- =============================================

USE DataWarehouse;
GO

DROP PROCEDURE IF EXISTS silver.sp_load_crm_sales;
GO

CREATE PROCEDURE silver.sp_load_crm_sales
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @start_time DATETIME2, @end_time DATETIME2;
    DECLARE @table_name NVARCHAR(100) = 'silver.crm_sales_details';
    DECLARE @rows_affected INT;
    DECLARE @rows_before_truncate INT;

    BEGIN TRY
        
        -- TRUNCATE Phase
        SET @start_time = GETDATE();
        SELECT @rows_before_truncate = COUNT(*) FROM silver.crm_sales_details;
        TRUNCATE TABLE silver.crm_sales_details;
        SET @end_time = GETDATE();
        
        INSERT INTO silver.load_log (
            table_name, operation_type, load_start, load_end, status, rows_affected,
            error_message, error_number, error_severity
        )
        VALUES (
            @table_name, 'TRUNCATE', @start_time, @end_time, 'Succeeded', @rows_before_truncate,
            NULL, NULL, NULL
        );
        
        PRINT @table_name + ': Truncated ' + CAST(@rows_before_truncate AS NVARCHAR(20)) + ' rows';
        
        -- INSERT Phase
        SET @start_time = GETDATE();
        
        INSERT INTO silver.crm_sales_details (
            sls_ord_num, sls_prd_key, sls_cust_id, sls_order_dt, sls_ship_dt, sls_due_dt,
            sls_sales, sls_quantity, sls_price
        )
        SELECT 
            sls_ord_num,
            sls_prd_key,
            TRY_CAST(sls_cust_id AS INT) AS sls_cust_id,
            
            CASE 
               
                WHEN a.sls_order_dt IS NOT NULL
                  AND YEAR(a.sls_order_dt) >= 1990
                  AND YEAR(a.sls_order_dt) <= YEAR(GETDATE()) + 1
                THEN a.sls_order_dt
                
                WHEN (a.sls_order_dt IS NULL 
                   OR YEAR(a.sls_order_dt) < 1990
                   OR YEAR(a.sls_order_dt) > YEAR(GETDATE()) + 1)
                  AND a.sls_ship_dt IS NOT NULL
                  AND YEAR(a.sls_ship_dt) >= 1990
                  AND YEAR(a.sls_ship_dt) <= YEAR(GETDATE()) + 1
                THEN DATEADD(DAY, -7, a.sls_ship_dt)
                
                ELSE NULL
            END AS sls_order_dt,
            
            CASE 
                WHEN a.sls_ship_dt IS NOT NULL 
                  AND (YEAR(a.sls_ship_dt) > YEAR(GETDATE()) + 1
                   OR YEAR(a.sls_ship_dt) < 1990)
                THEN NULL 
                ELSE a.sls_ship_dt 
            END AS sls_ship_dt,
            
            CASE 
                WHEN a.sls_due_dt IS NOT NULL 
                  AND (YEAR(a.sls_due_dt) > YEAR(GETDATE()) + 1
                   OR YEAR(a.sls_due_dt) < 1990)
                THEN NULL 
                ELSE a.sls_due_dt 
            END AS sls_due_dt,
            
            -- Business logic: Recalculate sales if inconsistent
            CASE 
                WHEN b.sls_sales IS NULL 
                  OR b.sls_sales <= 0 
                  OR b.sls_sales <> (b.sls_quantity * b.sls_price)
                THEN b.sls_quantity * b.sls_price
                ELSE b.sls_sales
            END AS sls_sales,

            b.sls_quantity AS sls_quantity,
            
            -- Business logic: Derive price if missing
            CASE 
                WHEN b.sls_price IS NULL OR b.sls_price <= 0 
                THEN b.sls_sales / NULLIF(b.sls_quantity, 0)
                ELSE b.sls_price
            END AS sls_price

        FROM bronze.crm_sales_details

        CROSS APPLY (
            SELECT
                TRY_CAST(sls_order_dt AS DATE) AS sls_order_dt,
                TRY_CAST(sls_ship_dt AS DATE) AS sls_ship_dt,
                TRY_CAST(sls_due_dt AS DATE) AS sls_due_dt
        ) a

        CROSS APPLY (
            SELECT
                TRY_CAST(sls_sales AS DECIMAL(18,2)) AS sls_sales,
                TRY_CAST(sls_quantity AS INT) AS sls_quantity,
                TRY_CAST(sls_price AS DECIMAL(18,2)) AS sls_price
        ) b;
        
        SET @rows_affected = @@ROWCOUNT;
        SET @end_time = GETDATE();
        
        INSERT INTO silver.load_log (
            table_name, operation_type, load_start, load_end, status, rows_affected,
            error_message, error_number, error_severity
        )
        VALUES (
            @table_name, 'INSERT', @start_time, @end_time, 'Succeeded', @rows_affected,
            NULL, NULL, NULL
        );
        
        PRINT @table_name + ': Inserted ' + CAST(@rows_affected AS NVARCHAR(20)) + ' rows';
            
    END TRY
    BEGIN CATCH
        
        SET @end_time = GETDATE();
        
        INSERT INTO silver.load_log (
            table_name, operation_type, load_start, load_end, status, rows_affected,
            error_message, error_number, error_severity
        )
        VALUES (
            @table_name, 'FAILED', @start_time, @end_time, 'Failed', 0,
            ERROR_MESSAGE(), ERROR_NUMBER(), ERROR_SEVERITY()
        );
        
        PRINT 'ERROR loading ' + @table_name + ': ' + ERROR_MESSAGE();
        THROW;
        
    END CATCH
END;
GO
