/*
===============================================================================
Stored Procedure: bronze.load_bronze
===============================================================================
Purpose:
    Loads data from external CSV files into the 'bronze' schema. 
    The procedure performs the following actions:
    - Truncates all existing bronze tables to avoid duplicate data.
    - Loads data into each table using the BULK INSERT command.

Parameters:
    None. This procedure accepts no parameters and returns no values.

Usage:
    EXEC bronze.load_bronze;

Notes:
    - Ensure file paths are accessible to the SQL Server instance.
    - CSV files must have headers (skipped using FIRSTROW = 2).
===============================================================================
*/

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME; 
	
	BEGIN TRY
		SET @batch_start_time = GETDATE();
		PRINT '================================================';
		PRINT 'Starting Bronze Layer Load';
		PRINT '================================================';

		-- ================================
		-- CRM Table Loads
		-- ================================
		PRINT 'Loading CRM Tables...';

		-- Load bronze.crm_cust_info
		SET @start_time = GETDATE();
		PRINT '>> Truncating: bronze.crm_cust_info';
		TRUNCATE TABLE bronze.crm_cust_info;
		PRINT '>> Inserting: bronze.crm_cust_info';
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\sql\dwh_project\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' sec';

		-- Load bronze.crm_prd_info
		SET @start_time = GETDATE();
		PRINT '>> Truncating: bronze.crm_prd_info';
		TRUNCATE TABLE bronze.crm_prd_info;
		PRINT '>> Inserting: bronze.crm_prd_info';
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\sql\dwh_project\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' sec';

		-- Load bronze.crm_sales_details
		SET @start_time = GETDATE();
		PRINT '>> Truncating: bronze.crm_sales_details';
		TRUNCATE TABLE bronze.crm_sales_details;
		PRINT '>> Inserting: bronze.crm_sales_details';
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\sql\dwh_project\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' sec';

		-- ================================
		-- ERP Table Loads
		-- ================================
		PRINT 'Loading ERP Tables...';

		-- Load bronze.erp_loc_a101
		SET @start_time = GETDATE();
		PRINT '>> Truncating: bronze.erp_loc_a101';
		TRUNCATE TABLE bronze.erp_loc_a101;
		PRINT '>> Inserting: bronze.erp_loc_a101';
		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\sql\dwh_project\datasets\source_erp\loc_a101.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' sec';

		-- Load bronze.erp_cust_az12
		SET @start_time = GETDATE();
		PRINT '>> Truncating: bronze.erp_cust_az12';
		TRUNCATE TABLE bronze.erp_cust_az12;
		PRINT '>> Inserting: bronze.erp_cust_az12';
		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\sql\dwh_project\datasets\source_erp\cust_az12.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' sec';

		-- Load bronze.erp_px_cat_g1v2
		SET @start_time = GETDATE();
		PRINT '>> Truncating: bronze.erp_px_cat_g1v2';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;
		PRINT '>> Inserting: bronze.erp_px_cat_g1v2';
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\sql\dwh_project\datasets\source_erp\px_cat_g1v2.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' sec';

		-- Final Summary
		SET @batch_end_time = GETDATE();
		PRINT '================================================';
		PRINT 'Bronze Layer Load Completed';
		PRINT 'Total Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' sec';
		PRINT '================================================';

	END TRY
	BEGIN CATCH
		PRINT '================================================';
		PRINT 'ERROR during Bronze Layer Load';
		PRINT 'Message: ' + ERROR_MESSAGE();
		PRINT 'Number: ' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'State: ' + CAST(ERROR_STATE() AS NVARCHAR);
		PRINT '================================================';
	END CATCH
END
