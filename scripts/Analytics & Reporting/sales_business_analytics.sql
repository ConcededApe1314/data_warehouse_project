/*
===============================================================================
SALES ANALYSIS SCRIPT
===============================================================================

Purpose:
    - This script contains a collection of queries designed to perform various
      types of sales analysis on the Gold layer of the data warehouse.
    - Each section focuses on a different analytical technique.

Sections:
    1. Change Over Time: Aggregating sales data across different time periods.
    2. Cumulative Analysis: Calculating running totals for sales.
    3. Performance Analysis: Comparing year-over-year product performance.
    4. Part to Whole Analysis: Calculating the contribution of categories to total sales.
    5. Data Segmentation: Grouping products and customers into segments.

===============================================================================
*/

/*
===============================================================================
1. CHANGE OVER TIME
===============================================================================
*/
-- Aggregate sales amount by day.
SELECT
	order_date,
	SUM(sales_amount) AS total_sales_amount
FROM
	gold.fact_sales
WHERE
	order_date IS NOT NULL
GROUP BY
	order_date
ORDER BY
	order_date;
GO

-- Aggregate sales by month, including unique customers and total quantity.
-- Note: This groups all Januarys, Februarys, etc., together, regardless of the year.
SELECT
	MONTH(order_date) AS order_date_month,
	SUM(sales_amount) AS total_sales_amount,
	COUNT(DISTINCT customer_key) AS total_customers,
	SUM(quantity) AS total_quantity
FROM
	gold.fact_sales
WHERE
	order_date IS NOT NULL
GROUP BY
	MONTH(order_date)
ORDER BY
	MONTH(order_date);
GO

-- Aggregate sales by year and month using DATETRUNC for a full date representation.
SELECT
	DATETRUNC(MONTH, order_date) AS order_date_trunc_month,
	SUM(sales_amount) AS total_sales_amount,
	COUNT(DISTINCT customer_key) AS total_customers,
	SUM(quantity) AS total_quantity
FROM
	gold.fact_sales
WHERE
	order_date IS NOT NULL
GROUP BY
	DATETRUNC(MONTH, order_date)
ORDER BY
	DATETRUNC(MONTH, order_date);
GO

-- Aggregate sales by year and month using separate YEAR and MONTH columns.
SELECT
	YEAR(order_date) AS order_date_year,
	MONTH(order_date) AS order_date_month,
	SUM(sales_amount) AS total_sales_amount,
	COUNT(DISTINCT customer_key) AS total_customers,
	SUM(quantity) AS total_quantity
FROM
	gold.fact_sales
WHERE
	order_date IS NOT NULL
GROUP BY
	YEAR(order_date),
	MONTH(order_date)
ORDER BY
	YEAR(order_date),
	MONTH(order_date);
GO


-- Aggregate sales by year, including unique customers and total quantity.
SELECT
	YEAR(order_date) AS order_date_year,
	SUM(sales_amount) AS total_sales_amount,
	COUNT(DISTINCT customer_key) AS total_customers,
	SUM(quantity) AS total_quantity
FROM
	gold.fact_sales
WHERE
	order_date IS NOT NULL
GROUP BY
	YEAR(order_date)
ORDER BY
	YEAR(order_date);
GO


/*
===============================================================================
2. CUMULATIVE ANALYSIS
===============================================================================
*/

-- Calculate the total sales per month and the running total of sales.
SELECT
	*,
	SUM(total_sales_amount) OVER (ORDER BY order_date_month) AS sales_running_total
FROM
	(
	SELECT
		DATETRUNC(MONTH, order_date) AS order_date_month,
		SUM(sales_amount) AS total_sales_amount
	FROM
		gold.fact_sales
	WHERE
		order_date IS NOT NULL
	GROUP BY
		DATETRUNC(MONTH, order_date)
	) AS subquery
ORDER BY
	order_date_month;
GO

-- Calculate the total sales per year and the running total of sales.
SELECT
	*,
	 SUM(total_sales_amount) OVER (ORDER BY order_date_year) AS running_total
FROM
	(
	SELECT
		DATETRUNC(YEAR, order_date) AS order_date_year,
		SUM(sales_amount) AS total_sales_amount
	FROM
		gold.fact_sales
	WHERE
		order_date IS NOT NULL
	GROUP BY
		DATETRUNC(YEAR, order_date)
	) AS subquery
ORDER BY
	order_date_year;
GO

/*
===============================================================================
3. PERFORMANCE ANALYSIS
===============================================================================
*/
-- Analyze the yearly performance of products by comparing their sales to both the product average sales performance and the previous year sales.
;WITH year_performance AS (
	-- Step 1: Aggregate product sales by year.
	SELECT
		YEAR(s.order_date) AS order_year,
		p.product_name,
		SUM(s.sales_amount) AS sales_by_year
	FROM
		gold.fact_sales AS s
	LEFT JOIN
		gold.dim_products AS p
	ON
		s.product_key = p.product_key
	WHERE
		s.order_date IS NOT NULL
	GROUP BY
		YEAR(s.order_date),
		p.product_name
)
-- Step 2: Calculate performance KPIs for each product-year combination.
SELECT
	order_year,
	product_name,
	sales_by_year,
	-- Calculates the historical average sales for each product across all years.
	AVG(sales_by_year) OVER (PARTITION BY product_name) AS total_avg_sales,
	-- Calculates the difference between the current year's sales and the product's historical average.
	sales_by_year - AVG(sales_by_year) OVER (PARTITION BY product_name) AS avg_sales_diff,
	-- Segments the performance against the historical average.
	CASE
		WHEN sales_by_year - AVG(sales_by_year) OVER (PARTITION BY product_name) > 0 THEN 'Increase'
		WHEN sales_by_year - AVG(sales_by_year) OVER (PARTITION BY product_name) < 0 THEN 'Decrease'
		ELSE 'Average'
	END AS avg_change,
	-- Fetches the sales amount from the previous year for the same product.
	LAG(sales_by_year) OVER (PARTITION BY product_name ORDER BY order_year ASC) AS previous_year_sales,
	-- Compares the current year's sales to the previous year's sales.
	CASE
		WHEN sales_by_year - LAG(sales_by_year) OVER (PARTITION BY product_name ORDER BY order_year ASC) > 0 THEN 'Increase'
		WHEN sales_by_year - LAG(sales_by_year) OVER (PARTITION BY product_name ORDER BY order_year ASC) < 0 THEN 'Decrease'
		WHEN LAG(sales_by_year) OVER (PARTITION BY product_name ORDER BY order_year ASC) IS NULL THEN 'n/a'
		ELSE 'No change'
	END AS previous_year_difference
FROM
	year_performance
ORDER BY
	product_name,
	order_year;
GO


/*
===============================================================================
4. PART TO WHOLE ANALYSIS
===============================================================================
*/
-- Which categories contribute the most to overall sales?
;WITH overall_sales AS (
	-- Step 1: Calculate total sales for each product category.
	SELECT
		p.category,
		SUM(s.sales_amount) AS total_sales_amount
	FROM
		gold.fact_sales AS s
	LEFT JOIN
		gold.dim_products AS p
	ON s.product_key = p.product_key
	GROUP BY
		p.category
)
-- Step 2: Calculate the percentage of total sales for each category.
SELECT
	category,
	total_sales_amount,
	-- Calculates the grand total of sales across all categories.
	SUM(total_sales_amount) OVER () AS overall_sales,
	-- Calculates each category's contribution as a percentage of the grand total.
	CONCAT(ROUND((CAST(total_sales_amount AS FLOAT) / SUM(total_sales_amount) OVER () * 100), 2), '%') AS percentage_of_total
FROM
	overall_sales
ORDER BY
	percentage_of_total DESC;
GO

/*
===============================================================================
5. DATA SEGMENTATION
===============================================================================
*/
-- Segment products into cost ranges and count how many products fall into each segment.
;WITH cost_segmentation AS (
	-- Step 1: Segment products into cost ranges using a CASE statement.
	SELECT
		product_key,
		product_name,
		cost,
		CASE
			WHEN cost < 100 THEN 'Below 100'
			WHEN cost BETWEEN 100 AND 500 THEN '100-500'
			WHEN cost BETWEEN 500 AND 1000 THEN '500-1000'
			ELSE 'Above 1000'
		END AS cost_range
	FROM
		gold.dim_products
)
-- Step 2: Count the number of products in each segment.
SELECT
	cost_range,
	COUNT(product_key) AS total_products
FROM
	cost_segmentation
GROUP BY
	cost_range
ORDER BY
	cost_range ASC;
GO

/*
-- Group customers into three segments based on their spending behavior:
--	- VIP: Customers with at least 12 months of history and spending more than $5,000.
--	- Regular: Customers with at least 12 months of history but spending $5000 or less.
--	- New: Customers with a lifespan less than 12 months.
-- Find the total number of customers in each group.
*/
;WITH client_segmentation AS (
	-- Step 1: Calculate total spending and lifespan for each customer.
	SELECT
		customer_key,
		SUM(sales_amount) AS total_spending,
		DATEDIFF(MONTH, MIN(order_date), MAX(order_date)) AS lifespan
	FROM
		gold.fact_sales
	GROUP BY
		customer_key
)
-- Step 2: Segment customers and count the total in each segment.
SELECT
	COUNT(customer_key) AS total_customers,
	client_segments
FROM
	(
	-- Subquery to apply the segmentation logic.
	SELECT
		customer_key,
		total_spending,
		lifespan,
		CASE
			WHEN lifespan >= 12 AND total_spending > 5000 THEN 'VIP'
			WHEN lifespan >= 12 AND total_spending <= 5000 THEN 'Regular'
			ELSE 'New'
		END AS client_segments
	FROM
		client_segmentation
	) AS subquery
GROUP BY
	client_segments
ORDER BY
	client_segments;
GO
