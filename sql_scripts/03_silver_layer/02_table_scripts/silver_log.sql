-- =============================================
-- Silver Layer: Load Audit Table Creation
-- =============================================
-- Purpose: Create audit table to track all Silver layer ETL operations
-- Tracks: Operation type, timestamps, row counts, durations, errors
-- Usage: Monitoring, debugging, performance analysis
-- Run Order: Before Silver load procedures
-- =============================================
USE DataWarehouse;
GO

-- =============================================
-- Create Load Log Table
-- =============================================
DROP TABLE IF EXISTS silver.load_log;
GO

CREATE TABLE silver.load_log (
    load_id INT IDENTITY(1,1) PRIMARY KEY,
    table_name NVARCHAR(100),
    operation_type NVARCHAR(50),     -- TRUNCATE / INSERT / FAILED
    load_start DATETIME2(7),
    load_end DATETIME2(7),
    status NVARCHAR(50),             -- Succeeded / Failed
    rows_affected INT,
    error_message NVARCHAR(MAX),
    error_number INT,
    error_severity INT,
    duration_ms AS DATEDIFF_BIG(MILLISECOND, load_start, load_end)
);
GO

-- =============================================
-- Create Indexes for Query Performance
-- =============================================

-- Index for queries filtering by table and most recent transactions
CREATE NONCLUSTERED INDEX IX_load_log_table_date 
ON silver.load_log(table_name, load_start DESC);
GO

-- Filtered index for searhching failures
CREATE NONCLUSTERED INDEX IX_load_log_status_date 
ON silver.load_log(status, load_start DESC)
WHERE status = 'Failed';
GO
