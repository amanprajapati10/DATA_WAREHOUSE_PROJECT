/*
===============================================================================
Stored Procedure: silver.load_silver
===============================================================================
Purpose:
    This stored procedure loads cleansed and standardized data from the 'bronze' 
    layer into the 'silver' schema. It performs data validation, transformation, 
    and standardization across CRM and ERP datasets to ensure data consistency 
    before being used in the gold analytical layer.

Process Overview:
    1. Truncates existing silver tables to ensure a clean reload.
    2. Transforms and loads data from corresponding bronze tables:
        - CRM Data:
            • silver.crm_cust_info      → Cleanses customer attributes 
              (names, gender, marital status).
            • silver.crm_prd_info       → Standardizes product details 
              (category, line, cost, start/end dates).
            • silver.crm_sales_details  → Validates and recalculates sales data.
        - ERP Data:
            • silver.erp_cust_az12      → Standardizes customer gender and 
              validates birthdates.
            • silver.erp_loc_a101       → Cleans and maps country codes.
            • silver.erp_px_cat_g1v2    → Loads category and subcategory details.
    3. Captures start and end timestamps for each table load.
    4. Prints execution duration for performance tracking.
    5. Handles errors gracefully with TRY-CATCH logging.

Notes:
    - Run this procedure after successful ingestion into the bronze layer.
    - Each insert includes data standardization and quality checks.
    - Default timestamps (dwh_create_date) track ETL load time.
*/

CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
	DECLARE @start_time DATETIME,@end_time DATETIME,@batch_start_time DATETIME,@batch_end_time DATETIME;
	BEGIN TRY
			SET @batch_start_time = GETDATE()
			PRINT '===================================================================';
			PRINT 'LOADING SILVER LAYER';
			PRINT '===================================================================';

			PRINT '-------------------------------------------------------------------';
			PRINT 'Loading CRM tables';
			PRINT '-------------------------------------------------------------------';
			SET @start_time = GETDATE()
			PRINT '>> Truncating Table: silver.crm_cust_info';
			TRUNCATE TABLE silver.crm_cust_info;
			PRINT '>> Inserting Data Into: Silver.crm_cust_info';
			INSERT INTO silver.crm_cust_info(
				cst_id,
				cst_key,
				cst_firstname,
				cst_lastname,
				cst_marital_status,
				cst_gndr,
				cst_create_date
			)
			SELECT 
			cst_id,
			cst_key,
			TRIM(cst_firstname) AS cst_firstname,
			TRIM(cst_lastname) AS cst_lastname,
			CASE	
				WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
				WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
				ELSE 'n/a'
			END cst_marital_status,
			CASE 
				WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
				WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
				ELSE 'n/a'
			END cst_gndr,
			cst_create_date

			FROM (
				SELECT 
				*,
				ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
				FROM bronze.crm_cust_info
	
			) AS t
			WHERE flag_last = 1 and cst_id is not  null 
			SET @end_time = GETDATE();
			PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
			PRINT '>> -------------';

			-- Loading silver.crm_prd_info
			SET @start_time = GETDATE();
			PRINT '>> Truncating Table: silver.crm_prd_info';
			TRUNCATE TABLE silver.crm_prd_info;
			PRINT '>> Inserting Data Into: Silver.crm_prd_info';
			INSERT INTO silver.crm_prd_info(
				prd_id,
				cat_id,
				prd_key,
				prd_nm,
				prd_cost,
				prd_line,
				prd_start_dt,
				prd_end_dt
			)
			SELECT 
			prd_id,
			REPLACE(SUBSTRING(prd_key,1,5),'-','_') as cat_id, --Extract category ID
			SUBSTRING(prd_key,7,LEN(prd_key)) AS prd_key, -- Extract product key
			prd_nm,
			ISNULL(prd_cost,0) AS prd_cost,
			CASE 
				WHEN prd_line = 'M' THEN 'Mountain'
				WHEN prd_line = 'R' THEN 'Road'
				WHEN prd_line = 'S' THEN 'Other Sales'
				WHEN prd_line = 'T' THEN 'Toursing'
				ELSE 'n/a'
			END prd_line, --Map product line code to descriptive values
			CAST(prd_start_dt AS DATE) AS prd_start_dt,
			CAST(LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS DATE) AS prd_end_dt --Calculate end date as one day before the next start date
			FROM bronze.crm_prd_info
			SET @end_time = GETDATE();
			PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
			PRINT '>> -------------';

			-- Loading crm_sales_details
			SET @start_time = GETDATE();
			PRINT '>> Truncating Table: silver.crm_sales_details';
			TRUNCATE TABLE silver.crm_sales_details;
			PRINT '>> Inserting Data Into: Silver.crm_sales_details';
			INSERT INTO silver.crm_sales_details(
				sls_ord_num,
				sls_prd_key,
				sls_cust_id,
				sls_order_dt,
				sls_ship_dt,
				sls_due_dt,
				sls_sales,
				sls_quantity,
				sls_price
			)
			SELECT
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			CASE 
				WHEN sls_order_dt = 0 OR LEN(sls_order_dt) !=8 THEN NULL
				ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
			END AS sls_order_dt,
			CASE 
				WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) !=8 THEN NULL
				ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
			END AS sls_ship_dt,
			CASE 
				WHEN sls_due_dt = 0 OR LEN(sls_due_dt) !=8 THEN NULL
				ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
			END AS sls_due_dt,
			CASE 
				WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
				THEN sls_quantity * ABS(sls_price)
				ELSE sls_sales
			END AS sls_sales,
			sls_quantity,
			CASE 
				WHEN sls_price IS NULL OR sls_price <= 0
					THEN sls_sales / NULLIF(sls_quantity,0)
				ELSE sls_price
			END AS sls_price
			from bronze.crm_sales_details
			SET @end_time = GETDATE();
			PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
			PRINT '>> -------------';

			-- Loading erp_cust_az12
			SET @start_time = GETDATE();
			PRINT '>> Truncating Table: silver.erp_cust_az12';
			TRUNCATE TABLE silver.erp_cust_az12;
			PRINT '>> Inserting Data Into: Silver.erp_cust_az12';
			INSERT INTO silver.erp_cust_az12(
				cid,
				bdate,
				gen
			)
			SELECT
			CASE 
				WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,LEN(cid))
				ELSE cid
			END AS cid,
			CASE
				WHEN bdate > GETDATE() THEN NULL
				ELSE bdate
			END AS bdate,
			CASE 
				WHEN UPPER(TRIM(gen)) IN ('F','FEMALE') THEN 'Female'
				WHEN UPPER(TRIM(gen)) IN ('M','MALE') THEN 'Male'
				ELSE 'n/a'
			END AS gen
			from bronze.erp_cust_az12
			SET @end_time = GETDATE();
			PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
			PRINT '>> -------------';

			-- Loading erp_loc_a101
			SET @start_time = GETDATE();
			PRINT '>> Truncating Table: silver.erp_loc_a101';
			TRUNCATE TABLE silver.erp_loc_a101;
			PRINT '>> Inserting Data Into: Silver.erp_loc_a101';
			INSERT INTO silver.erp_loc_a101(
				cid,
				cntry
			)
			SELECT
			REPLACE(cid,'-','') AS cid,
			CASE 
				WHEN TRIM(cntry) = 'DE' THEN 'Germany'
				WHEN TRIM(cntry) IN ('US','USA') THEN 'United States'
				WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
				ELSE TRIM(cntry)
			END as cntry
			FROM bronze.erp_loc_a101
			SET @end_time = GETDATE();
			PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
			PRINT '>> -------------';

			-- Loading erp_px_cat_g1v2
			SET @start_time = GETDATE();
			PRINT '>> Truncating Table: silver.erp_px_cat_g1v2';
			TRUNCATE TABLE silver.erp_px_cat_g1v2;
			PRINT '>> Inserting Data Into: Silver.erp_px_cat_g1v2';
			INSERT INTO silver.erp_px_cat_g1v2(id,cat,subcat,maintenance)
			SELECT
			id,
			cat,
			subcat,
			maintenance
			FROM 
			bronze.erp_px_cat_g1v2
			SET @end_time = GETDATE();
			PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
			PRINT '>> -------------';

			SET @batch_end_time = GETDATE();
			PRINT '=========================================='
			PRINT 'Loading Silver Layer is Completed';
			PRINT '   - Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
			PRINT '=========================================='
		
		END TRY
		BEGIN CATCH
			PRINT '=========================================='
			PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
			PRINT 'Error Message' + ERROR_MESSAGE();
			PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
			PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
			PRINT '=========================================='
	END CATCH
END