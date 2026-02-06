-- =============================================
-- Silver Layer: Automated Load Procedure - ERP Customer
-- =============================================
-- Purpose: Load cleaned ERP customer data from Bronze to Silver layer
-- Strategy: Full reload - TRUNCATE + INSERT with key parsing
-- Source: bronze.erp_cust_az12
-- Target: silver.erp_cust_az12
-- Logging: All operations logged to silver.load_log
-- =============================================

USE DataWarehouse;
GO

DROP PROCEDURE IF EXISTS silver.sp_load_erp_customer;
GO

CREATE PROCEDURE silver.sp_load_erp_customer
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @start_time DATETIME2, @end_time DATETIME2;
    DECLARE @table_name NVARCHAR(100) = 'silver.erp_cust_az12';
    DECLARE @rows_affected INT;
    DECLARE @rows_before_truncate INT;

    BEGIN TRY
        
        -- TRUNCATE Phase
        SET @start_time = GETDATE();
        SELECT @rows_before_truncate = COUNT(*) FROM silver.erp_cust_az12;
        TRUNCATE TABLE silver.erp_cust_az12;
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
        
        INSERT INTO silver.erp_cust_az12 (CID, BDATE, GEN)
        SELECT
            -- Key parsing: Remove 'NAS' prefix
            CASE
                WHEN CID LIKE 'NAS%' THEN SUBSTRING(CID, 4, LEN(CID))
                ELSE CID
            END AS CID,
            
            -- Date validation: Set future birthdates to NULL
            CASE
                WHEN TRY_CAST(BDATE AS DATE) > CAST(GETDATE() AS DATE) THEN NULL
                ELSE TRY_CAST(BDATE AS DATE)
            END AS BDATE,
            
            -- Standardization: Expand gender codes
            CASE
                WHEN LOWER(GEN) IN ('f', 'female') THEN 'Female'
                WHEN LOWER(GEN) IN ('m', 'male') THEN 'Male'
                ELSE 'Unknown'
            END AS GEN
        FROM bronze.erp_cust_az12;
        
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
