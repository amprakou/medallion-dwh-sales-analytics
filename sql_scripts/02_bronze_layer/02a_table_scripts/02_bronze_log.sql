-- =============================================
-- Bronze Layer: Load Audit Table Creation
-- =============================================
-- Purpose: Create audit table to track all Bronze layer ETL operations
-- Tracks: Operation type, timestamps, row counts, durations, errors
-- Usage: Monitoring, debugging
-- Run Order: After Bronze tables creation (01_bronze_ddl.sql)

-- =============================================
USE DataWarehouse;
GO

-- =============================================
-- Step 1: Create Load Log Table
-- =============================================
DROP TABLE IF EXISTS bronze.load_log;
GO

CREATE TABLE bronze.load_log (
    load_id INT IDENTITY(1,1) PRIMARY KEY,
    table_name NVARCHAR(100),
    type NVARCHAR(50),              -- TRUNCATE / INSERT / FAILED
    load_start DATETIME2(7),
    load_end DATETIME2(7),
    status NVARCHAR(50),            -- Succeeded / Failed
    rows_affected INT,
    error_message NVARCHAR(MAX),
    error_number INT,
    error_severity INT,
    duration_ms AS DATEDIFF_BIG(MILLISECOND, load_start, load_end)
);
GO

-- =============================================
-- Step 2: Create Indexes for Query Performance
-- =============================================

-- Index for queries filtering by table and most recent transactions
CREATE NONCLUSTERED INDEX IX_load_log_table_date 
ON bronze.load_log(table_name, load_start DESC);
GO

-- Filtered index for searhching failures
CREATE NONCLUSTERED INDEX IX_load_log_status_date 
ON bronze.load_log(status, load_start DESC)
WHERE status = 'Failed';
GO
