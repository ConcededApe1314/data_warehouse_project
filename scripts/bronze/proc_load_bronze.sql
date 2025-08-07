-- Creates a new procedure if it doesn't exist, or alters it if it already does.
CREATE OR ALTER PROCEDURE bronze.load_bronze AS
-- Marks the beginning of the block of code that defines the procedure's logic.
BEGIN
	DECLARE @batch_start_time DATETIME, @batch_end_time DATETIME, @start_time DATETIME, @end_time DATETIME;
	-- Starts a block of code that will be monitored for errors.
	-- If an error occurs inside this TRY block, control is immediately passed to the CATCH block.
	BEGIN TRY
		SET @batch_start_time = GETDATE();
		PRINT '========================================================================';
		PRINT 'Loading Bronze Layer'
		PRINT '========================================================================';

		--=============================================================================
		-- CRM Tables
		--=============================================================================
		PRINT '------------------------------------------------------------------------';
		PRINT 'Loading CRM Tables'
		PRINT '------------------------------------------------------------------------';
		--=============================================================================
		-- Table 1: bronze.crm_cust_info
		--=============================================================================

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_cust_info';
		-- Empties the target table completely for a clean reload.
		TRUNCATE TABLE bronze.crm_cust_info;

		PRINT '>> Inserting Data Into Table: bronze.crm_cust_info';
		-- Loads data from the specified CSV file.
		BULK INSERT bronze.crm_cust_info
		-- Path to the source data file.
		FROM 'C:\Users\didie\OneDrive\Escritorio\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
			-- Skips the header row in the CSV file.
			FIRSTROW = 2,
			-- Defines the comma as the column separator.
			FIELDTERMINATOR = ',',
			-- Locks the table for a faster, minimally logged bulk insert.
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> --------------------------';

		--=============================================================================
		-- Table 2: bronze.crm_prd_info
		--=============================================================================
		
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_prd_info';
		TRUNCATE TABLE bronze.crm_prd_info;

		PRINT '>> Inserting Data Into Table: bronze.crm_prd_info';
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Users\didie\OneDrive\Escritorio\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> --------------------------';

		--=============================================================================
		-- Table 3: bronze.crm_sales_details
		--=============================================================================

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_sales_details';
		TRUNCATE TABLE bronze.crm_sales_details;

		PRINT '>> Inserting Data Into Table: bronze.crm_sales_details';
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Users\didie\OneDrive\Escritorio\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> --------------------------';

		--=============================================================================
		-- ERP Tables
		--=============================================================================
		PRINT '------------------------------------------------------------------------';
		PRINT 'Loading ERP Tables'
		PRINT '------------------------------------------------------------------------';
		--=============================================================================
		-- Table 4: bronze.erp_loc_a101
		--=============================================================================

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_loc_a101';
		TRUNCATE TABLE bronze.erp_loc_a101;

		PRINT '>> Inserting Data Into Table: bronze.erp_loc_a101';
		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\Users\didie\OneDrive\Escritorio\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> --------------------------';

		--=============================================================================
		-- Table 5: bronze.erp_cust_az12
		--=============================================================================

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_cust_az12';
		TRUNCATE TABLE bronze.erp_cust_az12;

		PRINT '>> Inserting Data Into Table: bronze.erp_cust_az12';
		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\Users\didie\OneDrive\Escritorio\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			LASTROW = 18485,
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> --------------------------';

		--=============================================================================
		-- Table 6: bronze.erp_px_cat_g1v2
		--=============================================================================

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_px_cat_g1v2';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;

		PRINT '>> Inserting Data Into Table: bronze.erp_px_cat_g1v2';
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\Users\didie\OneDrive\Escritorio\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds' 
		PRINT '>> --------------------------';


		SET @batch_end_time = GETDATE();
		PRINT '========================================================================';
		PRINT 'Bronze Layer Load Completed';
		PRINT '<< Total Load Duration: ' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
		PRINT '========================================================================';

	-- End of the monitored code block.
	END TRY
	-- Starts a block of code that executes only if an error occurred in the preceding TRY block.
	-- This is where error logging and handling logic should be placed.
	BEGIN CATCH
			PRINT '========================================================================';
			PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER';

			-- Captures and displays the specific error message.
			PRINT 'Error Message: ' + ERROR_MESSAGE();

			-- Captures and displays the specific error number.
			PRINT 'Error Number: ' + CAST(ERROR_NUMBER() AS NVARCHAR(20));

			-- Captures and displays the error state.
			PRINT 'Error State: ' + CAST(ERROR_STATE() AS NVARCHAR(20));
			PRINT '========================================================================';
	END CATCH
-- Marks the end of the procedure's definition.
END
