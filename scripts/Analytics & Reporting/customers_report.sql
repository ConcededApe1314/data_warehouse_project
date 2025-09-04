/*
===============================================================================
Customer Report
===============================================================================

Purpose:
	- This view creates a comprehensive, reusable report that consolidates
	  key customer metrics and behaviors for segmentation and analysis.

Highlights:
	1. Aggregates transactional data to provide a customer-level summary of
	   order history, spending, and product interaction.
	2. Calculates essential KPIs such as average order value, average
	   monthly spend, and customer recency.
	3. Segments customers into behavior-based categories (VIP, Regular, New)
	   and demographic age groups.

===============================================================================
*/

-- Ensures the view is dropped and recreated to always have the latest definition.
IF OBJECT_ID('gold.report_customers', 'V') IS NOT NULL
    DROP VIEW gold.report_customers;
GO

CREATE VIEW gold.report_customers AS


WITH base_query AS (
	-- Step 1: Base Query
	SELECT
		s.order_number,
		s.product_key,
		s.order_date,
		s.sales_amount,
		s.quantity,
		c.customer_key,
		c.customer_number,
		CONCAT(c.first_name, ' ', c.last_name) AS customer_name,
		DATEDIFF(YEAR, c.birthdate, GETDATE()) AS customer_age
	FROM
		gold.fact_sales AS s
	LEFT JOIN
		gold.dim_customers AS c
			ON s.customer_key = c.customer_key
	WHERE
		s.order_date IS NOT NULL
),
-- Step 2: Customer Aggregations - Calculate core metrics for each customer.
customer_aggregation AS (
	SELECT
		customer_key,
		customer_number,
		customer_name,
		customer_age,
		COUNT(DISTINCT order_number) AS total_orders,
		SUM(sales_amount) AS total_sales,
		SUM(quantity) AS total_quantity,
		COUNT(DISTINCT product_key) AS total_products,
		MIN(order_date) AS first_order,
		MAX(order_date) AS last_order,
		-- Calculates the customer's purchasing lifespan in months.
		DATEDIFF(MONTH, MIN(order_date), MAX(order_date)) AS [lifespan (months)]
	FROM
		base_query
	GROUP BY
		customer_key,
		customer_number,
		customer_name,
		customer_age
)
-- Step 3: Final KPIs - Calculate advanced metrics and segment customers.
SELECT
	customer_key,
	customer_number,
	customer_name,
	customer_age,
	-- Segments customers into predefined age brackets.
	CASE
		WHEN customer_age < 20 THEN 'Under 20'
		WHEN customer_age BETWEEN 20 AND 29 THEN '20-29'
		WHEN customer_age BETWEEN 30 AND 39 THEN '30-39'
		WHEN customer_age BETWEEN 40 AND 49 THEN '40-49'
		ELSE '50 and Above'
	END AS age_group,
	total_orders,
	total_sales,
	total_quantity,
	total_products,
	-- Calculates the average revenue generated per order.
	ROUND(CAST(total_sales AS FLOAT) / NULLIF(total_orders, 0), 2) AS avg_order_value,
	-- Calculates average monthly spend, handling customers with a 0-month lifespan.
	CASE
		WHEN [lifespan (months)] = 0 THEN total_sales
		ELSE ROUND(CAST(total_sales AS FLOAT) / [lifespan (months)], 2)
	END AS avg_monthly_spend,
	-- Segments customers based on their historical value and loyalty.
	CASE
		WHEN [lifespan (months)] >= 12 AND total_sales > 5000 THEN 'VIP'
		WHEN [lifespan (months)] >= 12 AND total_sales <= 5000 THEN 'Regular'
		ELSE 'New'
	END AS customer_segment,
	first_order,
	last_order,
	[lifespan (months)],
	-- Calculates how many months have passed since the customer's last order.
	DATEDIFF(MONTH, last_order, GETDATE()) AS [customer_recency (months)]
FROM
	customer_aggregation;
GO