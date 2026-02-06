
-- =============================================
-- Silver Layer: Automated Load Procedure - ERP Product Category
-- =============================================
-- Purpose: Load ERP product category data from Bronze to Silver layer
-- Strategy: Full reload - TRUNCATE + INSERT (direct transfer, no transformations)
-- Source: bronze.erp_px_cat_g1v2
-- Target: silver.erp_px_cat_g1v2
-- Logging: All operations logged to silver.load_log
-- =============================================

USE DataWarehouse;
GO

DROP PROCEDURE IF EXISTS silver.sp_load_erp_product_category;
GO

CREATE PROCEDURE silver.sp_load_erp_product_category
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @start_time DATETIME2, @end_time DATETIME2;
    DECLARE @table_name NVARCHAR(100) = 'silver.erp_px_cat_g1v2';
    DECLARE @rows_affected INT;
    DECLARE @rows_before_truncate INT;

    BEGIN TRY
        
        -- TRUNCATE Phase
        SET @start_time = GETDATE();
        SELECT @rows_before_truncate = COUNT(*) FROM silver.erp_px_cat_g1v2;
        TRUNCATE TABLE silver.erp_px_cat_g1v2;
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
        
        -- INSERT Phase (Direct transfer - clean dataset)
        SET @start_time = GETDATE();
        
        INSERT INTO silver.erp_px_cat_g1v2 (ID, CAT, SUBCAT, MAINTENANCE)
        SELECT ID, CAT, SUBCAT, MAINTENANCE
        FROM bronze.erp_px_cat_g1v2;
        
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
