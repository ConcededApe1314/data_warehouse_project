/*
===============================================================================
Quality Checks
===============================================================================
Script Purpose:
    This script performs various quality checks for data consistency, accuracy, 
    and standardization across the 'silver' layer. It includes checks for:
    - Null or duplicate primary keys.
    - Unwanted spaces in string fields.
    - Data standardization and consistency.
    - Invalid date ranges and orders.
    - Data consistency between related fields.

Usage Notes:
    - Run these checks after data loading Silver Layer.
    - Investigate and resolve any discrepancies found during the checks.
===============================================================================
*/

--=============================================================================
-- Table 1: crm_cust_info
-- Source: CRM
--=============================================================================
-- Check for nulls or duplicates in primary key --> (cst_id)
-- This query identifies any cst_id that is either NULL or appears more than once.
-- Expectation: No results.
SELECT
    cst_id,
    COUNT(*) AS repetitions_indicator
FROM
    silver.crm_cust_info
GROUP BY
    cst_id
HAVING
    COUNT(*) > 1 OR cst_id IS NULL;

-- Check for unwanted spaces --> (cst_firstname)
-- Finds values that contain leading or trailing whitespace.
-- Expectation: No results.
SELECT
    cst_firstname
FROM
    silver.crm_cust_info
WHERE
    cst_firstname != TRIM(cst_firstname);

-- Check for unwanted spaces --> (cst_lastname)
-- Expectation: No results.
SELECT
    cst_lastname
FROM
    silver.crm_cust_info
WHERE
    cst_lastname != TRIM(cst_lastname);

-- Check for unwanted spaces --> (cst_gndr)
-- Expectation: No results.
SELECT
    cst_gndr
FROM
    silver.crm_cust_info
WHERE
    cst_gndr != TRIM(cst_gndr);

-- Check for unwanted spaces --> (cst_key)
-- Expectation: No results.
SELECT
    cst_key
FROM
    silver.crm_cust_info
WHERE
    cst_key != TRIM(cst_key);

-- Check for standardization --> (cst_gndr)
-- Expectation: Only 'Male', 'Female' or 'n/a' values.
SELECT DISTINCT
    cst_gndr 
FROM
    silver.crm_cust_info;

-- Check for standardization --> (cst_marital_status)
-- Expectation: Only 'Single' and 'Married' values.
SELECT DISTINCT
    cst_marital_status
FROM
    silver.crm_cust_info;

--=============================================================================
-- Table 2: crm_prd_info
-- Source: CRM
--=============================================================================
-- Check for NULLS or duplicates in Primary Key --> (prd_id)
-- Expectation: No results.
SELECT
	prd_id,
	COUNT(*) AS count
FROM
	silver.crm_prd_info
GROUP BY
	prd_id
HAVING
	COUNT(*) > 1 or prd_id IS NULL;

-- Validate composite key structure --> (prd_key)
-- This column is a composite key used to link to other tables.
-- First 5 characters are the category id from erp_px_cat_g1v2.
-- Characters 7 to the last it's sls_prd_key from crm_sales_details.

-- crm_prd_info
SELECT
    prd_key
FROM
    bronze.crm_prd_info;

-- erp_px_cat_g1v2 (id) = cat_prd_id
SELECT
    id
FROM
    bronze.erp_px_cat_g1v2;

-- crm_sales_details (sls_prd_key)
SELECT
    sls_prd_key
FROM
    bronze.crm_sales_details;

-- Check for unwanted whitespaces --> (prd_nm)
-- Expectation: No results.
SELECT
    prd_nm
FROM
    silver.crm_prd_info
WHERE
    prd_nm != TRIM(prd_nm);

-- Check for NULLS or negative numbers --> (prd_cost)
-- Expectation: No results.
SELECT
    prd_cost
FROM
    silver.crm_prd_info
WHERE
    prd_cost IS NULL OR prd_cost < 0;

-- Data standarization and consistency --> (prd_line)
-- Selects product lines that match a specific case-sensitive list, are NULL, or have leading/trailing spaces.
-- Expectation: No results.
SELECT
    prd_line
FROM
    silver.crm_prd_info
WHERE
    prd_line COLLATE Latin1_General_CS_AS IN ('m', 'r', 's', 't') -- Specifies a case-sensitive collation to distinguish between upper and lower case.
    OR prd_line IS NULL
    OR prd_line != TRIM(prd_line);

-- Data validation --> (prd_end_dt)
-- The source columns are DATETIME but contain no time data; they should be cast to DATE.
-- Check for records where the start date is incorrectly later than the end date.
-- Expectation: No results.
SELECT
    *
FROM
    silver.crm_prd_info
WHERE
    prd_start_dt > prd_end_dt

--=============================================================================
-- Table 3: crm_sales_details
-- Source: CRM
--=============================================================================
-- Check for unwanted whitespaces --> (sls_ord_num)
-- Expectation: No results.
-- This query returns sls_ord_num values that have leading or trailing spaces.
SELECT
    sls_ord_num
FROM
	silver.crm_sales_details
WHERE
	sls_ord_num != TRIM(sls_ord_num);


-- Check for referential integrity --> (sls_prd_key)
-- Expectation: No results.
-- This query finds sls_prd_key values that do not exist in the prd_sls_key column of the silver.crm_prd_info table.
SELECT
    sls_prd_key
FROM
	silver.crm_sales_details
WHERE
	sls_prd_key NOT IN (SELECT prd_sls_key FROM silver.crm_prd_info);


-- Check for referential integrity --> (sls_cust_id)
-- Expectation: No results.
-- This query finds sls_cust_id values that do not exist in the cst_id column of the silver.crm_cust_info table.
SELECT
	sls_cust_id
FROM
	silver.crm_sales_details
WHERE	
	sls_cust_id NOT IN (SELECT cst_id FROM silver.crm_cust_info);


--Check for invalid dates --> (sls_order_dt)
-- Expectation: No results.
SELECT
    *
FROM
    silver.crm_sales_details
WHERE
	   sls_order_dt > sls_ship_dt
	OR sls_order_dt > sls_due_dt;

--Check for invalid dates and logical consistency --> (sls_ship_dt)
-- Expectation: No results.
SELECT
	*
FROM
	silver.crm_sales_details
WHERE
	sls_ship_dt > sls_due_dt; -- A shipment should not occur after the expected delivery date.

-- Check data consistency between: sales, quantity, price --> (sls_sales, sls_quantity, sls)
-->> sales = quantity * price
-->> Values must not be: NULL, zero or negative
SELECT
	*
FROM
	silver.crm_sales_details
WHERE
	   sls_sales != sls_quantity * sls_price
	OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
	OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price IS NULL
ORDER BY
	sls_sales, sls_quantity, sls_price

--=============================================================================
-- Table 4: erp_cust_az12
-- Source: ERP
--=============================================================================
-- Check for key format consistency --> (cid)
-- Expectation: No results
-- This query identifies customer keys that do not conform to the
-- standard 'AW' prefix from cst_key from silver.crm_cust_info.

-- Verify that non-compliant keys in the silver source table.
SELECT
    cid
FROM
    silver.erp_cust_az12
WHERE
    cid NOT LIKE 'AW%'

UNION

-- Verify that no non-compliant keys exist in the silver target table.
SELECT
    cst_key
FROM
    silver.crm_cust_info
WHERE
    cst_key NOT LIKE 'AW%';

    
-- Check for date range and validity --> (bdate)
-- Finds invalid birth dates that are in the future or implausibly old.
-- Expectation: No results.
SELECT
    bdate
FROM
    silver.erp_cust_az12
WHERE
    bdate > GETDATE()
    OR bdate < DATEADD(YEAR, -110, GETDATE());

-- Check for value consistency --> (gen)
-- This exploratory query lists all unique values in the 'gen' column
-- to identify variations that need to be standardized.
-- Expectation: 'Male', 'Female' or 'n/a' values
SELECT DISTINCT
    gen
FROM
    silver.erp_cust_az12;

--=============================================================================
-- Table 5: erp_loc_a101
-- Source: ERP
--=============================================================================
-- Check for referential integrity --> (cid)
-- Expectation: No results.
-- The cid column is equivalent to cst_key, but may contain hyphens that need to be removed.
-- This query finds cid values that do not exist in silver.crm_cust_info, highlighting these format inconsistencies.
SELECT
	cid
FROM
	silver.erp_loc_a101
WHERE
	cid NOT IN (SELECT cst_key FROM silver.crm_cust_info);

-- Check for value consistency --> (cntry)
-- Expectation: Full country names, with 'n/a' instead of NULL.
-- This query lists all unique values in the 'cntry' column.
SELECT DISTINCT
	cntry
FROM
	silver.erp_loc_a101
ORDER BY
	cntry;

--=============================================================================
-- Table 6: erp_px_cat_g1v2
-- Source: ERP
--=============================================================================
-- Check for referential integrity --> (id)
-- This query finds category IDs that exist in the category table but are not used in any product record in crm_prd_info.
-- Expectation: Only 'CO_PD' value.
SELECT
    id
FROM
    bronze.erp_px_cat_g1v2
WHERE
    id
NOT IN (SELECT prd_cat_id FROM silver.crm_prd_info);

-- Check for unwanted spaces --> (cat)
-- Finds values that contain leading or trailing whitespace.
-- Expectation: No results.
SELECT
    cat
FROM
    bronze.erp_px_cat_g1v2
WHERE cat != TRIM(cat);

-- Check for unwanted spaces --> (subcat)
-- Expectation: No results.
SELECT
    subcat
FROM
    bronze.erp_px_cat_g1v2
WHERE subcat != TRIM(subcat);

-- Check for unwanted spaces --> (maintenance)
-- Expectation: No results.
SELECT
    maintenance
FROM
    bronze.erp_px_cat_g1v2
WHERE
    maintenance != TRIM(maintenance);

-- Check for value consistency --> (cat)
-- Returns all unique category values to manually verify consistency.
-- Expectation: A clean, standardized list of category names.
SELECT DISTINCT
    cat
FROM
    bronze.erp_px_cat_g1v2;

-- Check for value consistency --> (subcat)
-- Expectation: A clean, standardized list of subcategory names.
SELECT DISTINCT
    subcat
FROM
    bronze.erp_px_cat_g1v2;

-- Check for value consistency --> (maintenance)
-- Expectation: Only 'Yes' or 'No' values.
SELECT DISTINCT
    maintenance
FROM
    bronze.erp_px_cat_g1v2;