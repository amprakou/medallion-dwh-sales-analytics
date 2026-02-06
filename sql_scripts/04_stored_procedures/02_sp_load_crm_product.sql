-- =============================================
-- Silver Layer: Automated Load Procedure - CRM Product
-- =============================================
-- Purpose: Load cleaned product data from Bronze to Silver layer 
-- Strategy: Full reload - TRUNCATE + INSERT with LEAD() for end dates
-- Source: bronze.crm_prd_info
-- Target: silver.crm_prd_info
-- Logging: All operations logged to silver.load_log
-- =============================================

USE DataWarehouse;
GO

DROP PROCEDURE IF EXISTS silver.sp_load_crm_product;
GO

CREATE PROCEDURE silver.sp_load_crm_product
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @start_time DATETIME2, @end_time DATETIME2;
    DECLARE @table_name NVARCHAR(100) = 'silver.crm_prd_info';
    DECLARE @rows_affected INT;
    DECLARE @rows_before_truncate INT;

    BEGIN TRY
        
        -- TRUNCATE Phase
        SET @start_time = GETDATE();
        SELECT @rows_before_truncate = COUNT(*) FROM silver.crm_prd_info;
        TRUNCATE TABLE silver.crm_prd_info;
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
        INSERT INTO silver.crm_prd_info (
            prd_id, cat_id, prd_key, prd_nm, prd_cost, prd_line, prd_start_dt, prd_end_dt
        )
        SELECT 
            prd_id,
            cat_id,
            parsed_prd_key AS prd_key,
            prd_nm,
            prd_cost,
            prd_line,
            prd_start_dt,
            DATEADD(DAY, -1, LEAD(prd_start_dt) OVER (
                PARTITION BY parsed_prd_key 
                ORDER BY prd_start_dt
            )) AS prd_end_dt
        FROM cleaning_products;
        
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
