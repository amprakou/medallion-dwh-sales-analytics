-- =============================================
-- Bronze Layer: Automated Load Procedure - CRM Product Info
-- =============================================
-- Purpose: Load raw product info data from CSV to Bronze layer
-- Strategy: Full reload - TRUNCATE + INSERT preserving source structure
-- Source: CSV file (prd_info.csv)
-- Target: bronze.crm_prd_info
-- Logging: All operations logged to bronze.load_log
-- =============================================

USE DataWarehouse;
GO

DROP PROCEDURE IF EXISTS bronze.load_crm_prd_info;
GO
CREATE OR ALTER PROCEDURE bronze.load_crm_prd_info
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @start_time DATETIME2, @end_time DATETIME2;
    DECLARE @table_name NVARCHAR(100);
    DECLARE @rows_affected INT;
    DECLARE @rows_before_truncate INT;
    
   BEGIN TRY
        SET @table_name = 'bronze.crm_prd_info';
        
        -- TRUNCATE PHASE
        SET @start_time = GETDATE();
        SELECT @rows_before_truncate = COUNT(*) FROM bronze.crm_prd_info;
        TRUNCATE TABLE bronze.crm_prd_info;
        SET @end_time = GETDATE();
        
        INSERT INTO bronze.load_log (
            table_name, type, load_start, load_end, status, rows_affected, 
            error_message, error_number, error_severity
        )
        VALUES (
            @table_name, 'TRUNCATE', @start_time, @end_time, 'Succeeded', @rows_before_truncate, 
            NULL, NULL, NULL
        );
        
        PRINT @table_name + ': Truncated ' + CAST(@rows_before_truncate AS NVARCHAR(20)) + ' rows';
        
        -- INSERT PHASE
        SET @start_time = GETDATE();
        
        BULK INSERT bronze.crm_prd_info
        FROM 'C:\Users\User\Desktop\DW Project\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
        WITH (
            FIELDTERMINATOR = ',',
            FIRSTROW = 2, 
            TABLOCK
        );

        SET @rows_affected = @@ROWCOUNT;
        SET @end_time = GETDATE();
        
        INSERT INTO bronze.load_log (
            table_name, type, load_start, load_end, status, rows_affected, 
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
        
        INSERT INTO bronze.load_log (
            table_name, type, load_start, load_end, status, rows_affected, 
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
