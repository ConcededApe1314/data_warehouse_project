/*
===============================================================================
Quality Checks
===============================================================================
Script Purpose:
    This script performs quality checks to validate the integrity, consistency, 
    and accuracy of the Gold Layer. These checks ensure:
    - Uniqueness of surrogate keys in dimension tables.
    - Referential integrity between fact and dimension tables.
    - Validation of relationships in the data model for analytical purposes.

Usage Notes:
    - Investigate and resolve any discrepancies found during the checks.
===============================================================================
*/

--=============================================================================
-- Checking 'gold.dim_customers'
--=============================================================================
-- Verify that each customer_key is unique.
-- Expectation: No results. 
SELECT 
    customer_key, -- Surrogate key
    COUNT(*) AS duplicate_count
FROM
    gold.dim_customers
GROUP BY
    customer_key
HAVING
    COUNT(*) > 1;

--=============================================================================
-- Checking 'gold.product_key'
--=============================================================================
-- Verify that each product_key is unique.
-- Expectation: No results.
SELECT 
    product_key, -- Surrogate key
    COUNT(*) AS duplicate_count
FROM
    gold.dim_products
GROUP BY
    product_key
HAVING
    COUNT(*) > 1;

--=============================================================================
-- Checking 'gold.fact_sales'
--=============================================================================
-- Check the data model relationship between fact and dimension views.
-- Expectation: No results 
SELECT
    *
FROM
    gold.fact_sales f
LEFT JOIN
    gold.dim_customers c
ON
    c.customer_key = f.customer_key
LEFT JOIN
    gold.dim_products p
ON
    p.product_key = f.product_key
WHERE
    p.product_key IS NULL OR c.customer_key IS NULL
