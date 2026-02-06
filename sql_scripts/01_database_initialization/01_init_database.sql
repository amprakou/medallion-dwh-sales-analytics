-- =============================================
-- Database Initialization Script
-- =============================================
-- Purpose: Create database and schemas for this project
-- Creates: Database and schemas (Bronze/Silver/Gold)
-- Run Order: First script to execute
-- =============================================

USE master;
GO

-- =============================================
-- Step 1: Create Database
-- =============================================

CREATE DATABASE DataWarehouse;
GO

USE DataWarehouse;
GO

-- =============================================
-- Step 2: Create Schemas (Medallion Architecture)
-- =============================================

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'bronze')
    EXEC('CREATE SCHEMA bronze');
GO

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'silver')
    EXEC('CREATE SCHEMA silver');
GO

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'gold')
    EXEC('CREATE SCHEMA gold');
GO
