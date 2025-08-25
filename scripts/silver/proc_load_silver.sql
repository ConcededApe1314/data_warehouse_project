/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'silver' schema by cleaning and
    transforming data from the 'bronze' schema. It performs the following actions:
    - Truncates the silver tables before loading data.
    - Cleans and standardizes text data (e.g., TRIM, CASE).
    - Transforms data types (e.g., integer to DATE).
    - Deduplicates records to keep only the most recent version.
    - Calculates new fields based on business logic (e.g., prd_end_dt).

Parameters:
    None. This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC silver.load_silver;
===============================================================================
*/

CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
    DECLARE @batch_start_time DATETIME, @batch_end_time DATETIME, @start_time DATETIME, @end_time DATETIME;
    BEGIN TRY
        SET @batch_start_time = GETDATE();
        PRINT '========================================================================';
        PRINT '>> Loading Silver Layer';
        PRINT '========================================================================';
        --=============================================================================
		-- CRM Tables
		--=============================================================================
        PRINT '------------------------------------------------------------------------';
        PRINT '>> Loading CRM Tables'
        PRINT '------------------------------------------------------------------------';
        --=============================================================================
        -- Load Table: silver.crm_cust_info
        --=============================================================================

        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: silver.crm_cust_info';
        -- Clear the target table for a full reload.
        TRUNCATE TABLE silver.crm_cust_info;

        PRINT '>> Inserting Data Into: silver.crm_cust_info';
        -- Insert cleaned and transformed data from the Bronze layer.
        INSERT INTO silver.crm_cust_info (
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
            -- Clean leading/trailing whitespace from names.
            TRIM(cst_firstname) AS cst_firstname,
            TRIM(cst_lastname) AS cst_lastname,
            -- Standardize marital status values.
            CASE
                WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
                WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
                ELSE 'n/a'
            END AS cst_marital_status,
            -- Standardize gender values.
            CASE
                WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
                WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
                ELSE 'n/a'
            END AS cst_gndr,
            cst_create_date
        FROM
            (
            -- Subquery to find the most recent record for each customer.
            SELECT
                *,
                -- Rank records for each customer to find the most recent one (#1).
                ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) recent_cst_create_date
            FROM
                bronze.crm_cust_info
            WHERE
                cst_id IS NOT NULL
            ) AS sub
        -- Filter for the most recent record for each customer.
        WHERE recent_cst_create_date = 1;

        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> --------------------------';

        --=============================================================================
        -- Load Table: silver.crm_prd_info
        --=============================================================================

        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: silver.crm_prd_info';
        -- Clear the target table for a full reload.
        TRUNCATE TABLE silver.crm_prd_info;

        PRINT '>> Inserting Data Into: silver.crm_prd_info';
        -- Insert cleaned and transformed data.
        INSERT INTO silver.crm_prd_info (
            prd_id,
            prd_cat_id,
            prd_sls_key,
            prd_nm,
            prd_cost,
            prd_line,
            prd_start_dt,
            prd_end_dt
        )
        SELECT
	        prd_id,
            -- Extract and format the category ID from prd_key.
            REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS prd_cat_id,
            -- Extract the sales key from prd_key.
            SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_sls_key,
            prd_nm,
            -- Replace NULL costs with 0.
            COALESCE(prd_cost, 0) AS prd_cost,
            -- Standardize product line values.
            CASE
                WHEN prd_line = 'M' THEN 'Mountain'
                WHEN prd_line = 'R' THEN 'Road'
                WHEN prd_line = 'S' THEN 'Other Sales'
                WHEN prd_line = 'T' THEN 'Touring'
                ELSE 'n/a'
            END AS prd_line,
            -- Cast start date to DATE to remove time component.
            CAST(prd_start_dt AS DATE) AS prd_start_dt,
            -- Calculate the end date based on the start date of the next record.
            CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt ASC) - 1 AS DATE) AS prd_end_dt
        FROM
            bronze.crm_prd_info;

        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> --------------------------';

        --=============================================================================
        -- Load Table: silver.crm_sales_details
        --=============================================================================
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: silver.crm_sales_details';
        -- Clear the target table for a full reload.
        TRUNCATE TABLE silver.crm_sales_details;

        PRINT '>> Inserting Data Into: silver.crm_sales_details';
        -- Insert cleaned and transformed data.
        INSERT INTO silver.crm_sales_details (
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
            -- Safely convert integer dates to DATE format, returning NULL on failure.
            CASE
                WHEN LEN(sls_order_dt) = 8 THEN TRY_CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
                ELSE NULL
            END AS sls_order_dt,
             CASE
                WHEN LEN(sls_ship_dt) = 8 THEN TRY_CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
                ELSE NULL
            END AS sls_ship_dt,
            CASE
                WHEN LEN(sls_due_dt) = 8 THEN TRY_CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
                ELSE NULL
            END AS sls_due_dt,
            -- Validate and recalculate sls_sales if inconsistent or invalid.
            CASE
                WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != ABS(sls_quantity * sls_price)
                    THEN ABS(sls_quantity * sls_price)
                ELSE sls_sales
            END AS sls_sales,
            -- Ensure sls_quantity is a positive value.
            CASE
                WHEN sls_quantity < 0
                    THEN ABS(sls_quantity)
                ELSE sls_quantity
            END AS sls_quantity,
            -- Recalculate sls_price if it is missing or invalid.
            CASE
                WHEN sls_price IS NULL OR sls_price <= 0
                    THEN ABS(sls_sales / NULLIF(sls_quantity, 0))
                ELSE sls_price
            END AS sls_price
        FROM
	        bronze.crm_sales_details;

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
        -- Load Table: silver.erp_cust_az12
        --=============================================================================
        
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: silver.erp_cust_az12';
        -- Clear the target table for a full reload.
        TRUNCATE TABLE silver.erp_cust_az12;

        PRINT '>> Inserting Data Into: silver.erp_cust_az12';
        -- Insert cleaned and transformed data.
        INSERT INTO silver.erp_cust_az12 (
            cid,
            bdate,
            gen
        )
        SELECT
            -- Remove the 'NAS' prefix from cid values where it exists.
            CASE
                WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
                ELSE cid
            END AS cid,
            -- Set future birth dates to NULL.
            CASE
                WHEN bdate > GETDATE() THEN NULL
                ELSE bdate
            END AS bdate,
            -- Standardize gender values.
            CASE
                WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
                WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
                ELSE 'n/a'
            END AS gen
        FROM
            bronze.erp_cust_az12;

        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> --------------------------';

        --=============================================================================
        -- Load Table: silver.erp_loc_a101
        --=============================================================================
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: silver.erp_loc_a101';
        -- Clear the target table for a full reload.
        TRUNCATE TABLE silver.erp_loc_a101;

        PRINT '>> Inserting Data Into: silver.erp_loc_a101';
        -- Insert cleaned and transformed data.
        INSERT INTO silver.erp_loc_a101 (
            cid,
            cntry
        )
        SELECT
            -- Remove hyphens from cid to match the standard key format.
	        REPLACE(cid, '-', '') AS cid,
            -- Standardize country names by expanding abbreviations and handling NULL/blank values.
	        CASE
		        WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
		        WHEN TRIM(cntry) = 'DE' THEN 'Germany'
		        WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
		        ELSE cntry
	        END AS cntry
        FROM
	        bronze.erp_loc_a101;

        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> --------------------------';

        --=============================================================================
        -- Load Table: silver.erp_px_cat_g1v2
        --=============================================================================
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: silver.erp_px_cat_g1v2';
        -- Clear the target table for a full reload.
        TRUNCATE TABLE silver.erp_px_cat_g1v2;

        PRINT '>> Inserting Data Into: silver.erp_px_cat_g1v2';
        -- Insert data directly from Bronze, as no transformations are needed for this table.
        INSERT INTO silver.erp_px_cat_g1v2 (
            id,
	        cat,
	        subcat,
	        maintenance
        )
        SELECT
	        id,
	        cat,
	        subcat,
	        maintenance
        FROM
	        bronze.erp_px_cat_g1v2;

        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> --------------------------';


        SET @batch_end_time = GETDATE();
        PRINT '========================================================================';
        PRINT '>> Silver Layer Load Completed';
		PRINT '>> Total Load Duration: ' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
        PRINT '========================================================================';
    
    END TRY

    BEGIN CATCH
		PRINT '========================================================================';
		PRINT 'ERROR OCCURED DURING LOADING SILVER LAYER'; -- Corrected from BRONZE to SILVER

		-- Captures and displays the specific error message.
		PRINT 'Error Message: ' + ERROR_MESSAGE();

		-- Captures and displays the specific error number.
		PRINT 'Error Number: ' + CAST(ERROR_NUMBER() AS NVARCHAR(20));

		-- Captures and displays the error state.
		PRINT 'Error State: ' + CAST(ERROR_STATE() AS NVARCHAR(20));
		PRINT '========================================================================';
	END CATCH
END;
