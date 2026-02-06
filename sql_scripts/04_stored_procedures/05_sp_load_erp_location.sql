-- =============================================
-- Silver Layer: Automated Load Procedure - ERP Location
-- =============================================
-- Purpose: Load cleaned ERP location data from Bronze to Silver layer
-- Strategy: Full reload - TRUNCATE + INSERT with key parsing
-- Source: bronze.erp_loc_a101
-- Target: silver.erp_loc_a101
-- Logging: All operations logged to silver.load_log
-- =============================================

USE DataWarehouse;
GO

DROP PROCEDURE IF EXISTS silver.sp_load_erp_location;
GO

CREATE PROCEDURE silver.sp_load_erp_location
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @start_time DATETIME2, @end_time DATETIME2;
    DECLARE @table_name NVARCHAR(100) = 'silver.erp_loc_a101';
    DECLARE @rows_affected INT;
    DECLARE @rows_before_truncate INT;

    BEGIN TRY
        
        -- TRUNCATE Phase
        SET @start_time = GETDATE();
        SELECT @rows_before_truncate = COUNT(*) FROM silver.erp_loc_a101;
        TRUNCATE TABLE silver.erp_loc_a101;
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
        
        INSERT INTO silver.erp_loc_a101 (CID, CNTRY)
        SELECT
            -- Key parsing: Remove '-' delimiter
            REPLACE(CID, '-', '') AS CID,
            
            -- Standardization: Normalize country codes
            CASE
                WHEN CNTRY = 'DE' THEN 'Germany'
                WHEN CNTRY IN ('US', 'USA', 'United States') THEN 'United States'
                WHEN CNTRY = '' OR CNTRY IS NULL THEN 'Unknown'
                ELSE CNTRY
            END AS CNTRY
        FROM bronze.erp_loc_a101;
        
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
