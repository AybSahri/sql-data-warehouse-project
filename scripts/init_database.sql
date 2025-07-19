/*
==========================================================================
  Data Warehouse Initialization Script
==========================================================================
  
  This script performs the following actions:
  
  1. Checks if the 'DataWarehouse' database exists.
     - If it exists, sets it to SINGLE_USER mode and drops it to ensure a clean slate.
  2. Creates a fresh 'DataWarehouse' database.
  3. Switches context to the 'DataWarehouse' database.
  4. Creates three schemas representing different data layers:
     - bronze: Raw, ingested data
     - silver: Cleaned and transformed data
     - gold: Curated, analytics-ready data

  Usage:
  Run this script to reset and initialize the Data Warehouse environment.
  
  Warning:
  This will delete all existing data and objects in the 'DataWarehouse' database.
  Use with caution, primarily in development or testing environments.
==========================================================================
*/

USE master;

-- Check if the DataWarehouse database exists; if it does, set it to SINGLE_USER and drop it to ensure a clean initialization
IF EXISTS (SELECT name FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
    ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE DataWarehouse;
END

-- Create a fresh DataWarehouse database
CREATE DATABASE DataWarehouse;
GO

USE DataWarehouse;

-- Create the bronze schema for raw, ingested data
CREATE SCHEMA bronze;
GO

-- Create the silver schema for cleaned and transformed data
CREATE SCHEMA silver;
GO

-- Create the gold schema for curated, analytics-ready data
CREATE SCHEMA gold;
GO
