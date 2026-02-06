-- =============================================
-- Silver Layer: Automated Load Procedure - CRM Customer
-- =============================================
-- Purpose: Load cleaned customer data from Bronze to Silver layer
-- Strategy: Full reload - TRUNCATE + INSERT
-- Source: bronze.crm_cust_info
-- Target: silver.crm_cust_info
-- Logging: All operations logged to silver.load_log
-- =============================================

USE DataWarehouse;
GO

DROP PROCEDURE IF EXISTS silver.sp_load_crm_customer;
GO

CREATE PROCEDURE silver.sp_load_crm_customer
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @start_time DATETIME2, @end_time DATETIME2;
    DECLARE @table_name NVARCHAR(100) = 'silver.crm_cust_info';
    DECLARE @rows_affected INT;
    DECLARE @rows_before_truncate INT;

    BEGIN TRY
        
        -- =============================================
        -- TRUNCATE Phase
        -- =============================================
        
        SET @start_time = GETDATE();
        SELECT @rows_before_truncate = COUNT(*) FROM silver.crm_cust_info;
        TRUNCATE TABLE silver.crm_cust_info;
        SET @end_time = GETDATE();
        
        -- Log TRUNCATE
        INSERT INTO silver.load_log (
            table_name, operation_type, load_start, load_end, status, rows_affected,
            error_message, error_number, error_severity
        )
        VALUES (
            @table_name, 'TRUNCATE', @start_time, @end_time, 'Succeeded', @rows_before_truncate,
            NULL, NULL, NULL
        );
        
        PRINT @table_name + ': Truncated ' + CAST(@rows_before_truncate AS NVARCHAR(20)) + ' rows';
        
        -- =============================================
        -- INSERT Phase (Reuse transformation logic)
        -- =============================================
        
        SET @start_time = GETDATE();
        
        WITH cleaning_cust_info AS (
            SELECT 
                TRY_CAST(cst_id AS INT) AS cst_id,
                ROW_NUMBER() OVER (
                    PARTITION BY cst_id 
                    ORDER BY TRY_CAST(cst_create_date AS DATE) DESC
                ) AS rn,
                TRY_CAST(cst_key AS VARCHAR(15)) AS cst_key,
                TRIM(cst_firstname) AS cst_firstname,
                TRIM(cst_lastname) AS cst_lastname,
                CASE 
                    WHEN TRIM(cst_marital_status) = 'M' THEN 'Married'
                    WHEN TRIM(cst_marital_status) = 'S' THEN 'Single'
                    ELSE 'Unknown'
                END AS cst_marital_status,
                CASE 
                    WHEN TRIM(cst_gndr) = 'M' THEN 'Male'
                    WHEN TRIM(cst_gndr) = 'F' THEN 'Female'
                    ELSE 'Unknown'
                END AS cst_gndr,
                TRY_CAST(cst_create_date AS DATE) AS cst_create_date
            FROM bronze.crm_cust_info
        )
        INSERT INTO silver.crm_cust_info (
            cst_id, cst_key, cst_firstname, cst_lastname,
            cst_marital_status, cst_gndr, cst_create_date
        )
        SELECT 
            cst_id, cst_key, cst_firstname, cst_lastname,
            cst_marital_status, cst_gndr, cst_create_date
        FROM cleaning_cust_info
        WHERE rn = 1 AND cst_id IS NOT NULL;
        
        SET @rows_affected = @@ROWCOUNT;
        SET @end_time = GETDATE();
        
        -- Log INSERT
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
        
        -- Log FAILURE
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
