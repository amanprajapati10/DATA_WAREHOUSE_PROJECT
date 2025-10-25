/*
=============================================================
DDL Script: Create or Alter Stored Procedure - bronze.load_bronze
=============================================================

Script Purpose:
    This stored procedure loads data into all 'bronze' layer tables 
    (CRM and ERP datasets) from their respective CSV source files.

    It performs the following tasks:
        • Truncates existing data in bronze tables.
        • Bulk loads CSV data from local file paths.
        • Logs progress messages and calculates load durations.
        • Handles errors gracefully with TRY...CATCH.

    Run this procedure to refresh the Bronze layer 
    before transforming data into Silver and Gold layers 
    for visualization or analytics.

=============================================================
*/

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME,@end_time DATETIME,@batch_start_time DATETIME,@batch_end_time DATETIME;
	BEGIN TRY
		SET @batch_start_time = GETDATE()
		PRINT '===================================================================';
		PRINT 'LOADING BRONZE LAYER';
		PRINT '===================================================================';

		PRINT '-------------------------------------------------------------------';
		PRINT 'Loading CRM tables';
		PRINT '-------------------------------------------------------------------';
		
		SET @start_time = GETDATE()
		PRINT '>> Truncating Table : bronze.crm_cust_info' ;
		TRUNCATE TABLE bronze.crm_cust_info;
		PRINT '>> Inserting Data INTO : bronze.crm_cust_info'
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\Users\Asus\Desktop\SQL_PROJECT\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH(
			FIRSTROW =2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time,@end_time) AS NVARCHAR) + ' seconds';

		SET @start_time = GETDATE()
		PRINT '>> Truncating Table : bronze.crm_prd_info' ;
		TRUNCATE TABLE bronze.crm_prd_info;
		PRINT '>> Inserting Data INTO : bronze.crm_prd_info'
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Users\Asus\Desktop\SQL_PROJECT\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time,@end_time) AS NVARCHAR) + ' seconds';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table : bronze.crm_sales_details' ;
		TRUNCATE TABLE bronze.crm_sales_details;
		PRINT '>> Inserting Data INTO : bronze.crm_sales_details'
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Users\Asus\Desktop\SQL_PROJECT\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time,@end_time) AS NVARCHAR) + ' seconds';



		PRINT '-------------------------------------------------------------------';
		PRINT 'Loading ERP tables';
		PRINT '-------------------------------------------------------------------';
		
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table : bronze.erp_cust_az12' ;
		TRUNCATE TABLE bronze.erp_cust_az12;
		PRINT '>> Inserting Data INTO : bronze.erp_cust_az12'
		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\Users\Asus\Desktop\SQL_PROJECT\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time,@end_time) AS NVARCHAR) + ' seconds';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table : bronze.erp_loc_a101' ;
		TRUNCATE TABLE bronze.erp_loc_a101;
		PRINT '>> Inserting Data INTO : bronze.erp_loc_a101'
		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\Users\Asus\Desktop\SQL_PROJECT\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time,@end_time) AS NVARCHAR) + ' seconds';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table : bronze.erp_px_cat_g1v2' ;
		TRUNCATE TA
