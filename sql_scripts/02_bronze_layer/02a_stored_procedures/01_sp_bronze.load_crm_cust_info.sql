-- =============================================
-- Bronze Layer: Automated Load Procedure - CRM Customer
-- =============================================
-- Purpose: Load raw customer data from CSV to Bronze layer
-- Strategy: Full reload - TRUNCATE + INSERT preserving source structure
-- Source: CSV file (cust_info.csv)
-- Target: bronze.crm_cust_info
-- Logging: All operations logged to bronze.load_log
-- =============================================

USE DataWarehouse;
GO

DROP PROCEDURE IF EXISTS bronze.load_crm_cust_info;
GO
    
CREATE OR ALTER PROCEDURE bronze.load_crm_cust_info
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @start_time DATETIME2, @end_time DATETIME2;
    DECLARE @table_name NVARCHAR(100);
    DECLARE @rows_affected INT;
    DECLARE @rows_before_truncate INT;
    
  BEGIN TRY
        SET @table_name = 'bronze.crm_cust_info';
        
        -- TRUNCATE PHASE
        SET @start_time = GETDATE();
        SELECT @rows_before_truncate = COUNT(*) FROM bronze.crm_cust_info;
        TRUNCATE TABLE bronze.crm_cust_info;
        SET @end_time = GETDATE();
        
        -- Log TRUNCATE
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
        
        BULK INSERT bronze.crm_cust_info
        FROM 'C:\Users\User\Desktop\DW Project\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
        WITH (
            FIELDTERMINATOR = ',',
            FIRSTROW = 2, 
            TABLOCK
        );
        
        SET @rows_affected = @@ROWCOUNT;
        SET @end_time = GETDATE();
        
        -- Log INSERT
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
        
        -- Log FAILURE
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
