/*
===============================================================================
Product Report
===============================================================================

Purpose:
	- This view creates a comprehensive, reusable report that aggregates
	  product performance metrics and calculates key business KPIs.

Highlights:
	1. Aggregates transactional data to provide a product-level summary of
	   sales, orders, and quantity sold.
	2. Calculates essential KPIs such as average order revenue, average
	   monthly revenue, and product recency.
	3. Segments products into 'High-Performer', 'Mid-Performer', and
	   'Low-Performer' tiers based on their total sales.

===============================================================================
*/

-- Ensures the view is dropped and recreated to always have the latest definition.
IF OBJECT_ID('gold.report_products', 'V') IS NOT NULL
    DROP VIEW gold.report_products;
GO

CREATE VIEW gold.report_products AS

WITH base_query AS (
	-- Step 1: Base Query - Join and gather raw transactional data.
	SELECT
		s.order_number,
		s.product_key,
		s.sales_amount,
		s.quantity,
		s.price,
		p.product_name,
		p.category,
		p.subcategory,
		p.cost,
		s.order_date,
		s.customer_key
	FROM
		gold.fact_sales AS s
	LEFT JOIN
		gold.dim_products AS p
			ON s.product_key = p.product_key
	WHERE
		s.order_date IS NOT NULL
),
-- Step 2: Product Aggregations - Calculate core metrics per product.
product_aggregation AS (
	SELECT
		product_key,
		product_name,
		category,
		subcategory,
		price,
		COUNT(DISTINCT order_number) AS total_orders,
		SUM(sales_amount) AS total_sales,
		SUM(quantity) AS total_quantity_sold,
		-- Calculates the average selling price per unit.
		ROUND(AVG(CAST(sales_amount AS FLOAT) / NULLIF(quantity, 0)), 2) AS avg_selling_price,
		MIN(order_date) AS first_order_date,
		MAX(order_date) AS last_order_date,
		-- Calculates the product's sales lifespan in months.
		DATEDIFF(MONTH, MIN(order_date), MAX(order_date)) AS [lifespan (in months)],
		COUNT(DISTINCT customer_key) AS total_customers
	FROM
		base_query
	GROUP BY
		product_key,
		product_name,
		category,
		subcategory,
		price
)
-- Step 3: Final KPIs - Calculate advanced metrics and segment products.
SELECT
	product_key,
	product_name,
	category,
	subcategory,
	price,
	total_orders,
	total_quantity_sold,
	total_sales,
	avg_selling_price,
	-- Calculates the average revenue generated per order.
	ROUND((CAST(total_sales AS FLOAT) / NULLIF(total_orders, 0)), 2) AS avg_order_revenue,
	-- Calculates average monthly revenue, handling products with a 0-month lifespan.
	CASE
		WHEN [lifespan (in months)] = 0 THEN total_sales
		ELSE ROUND((CAST(total_sales AS FLOAT) / [lifespan (in months)]), 2)
	END AS avg_monthly_revenue,
	-- Segments products into three performance tiers based on total sales.
	CASE NTILE(3) OVER (ORDER BY total_sales DESC)
		WHEN 1 THEN 'High-Performer'
		WHEN 2 THEN 'Mid-Performer'
		ELSE 'Low-Performer'
	END AS product_segment,
	first_order_date,
	last_order_date,
	[lifespan (in months)],
	-- Calculates how many months have passed since the last sale.
	DATEDIFF(MONTH, last_order_date, GETDATE()) AS [product_recency (in months)],
	total_customers
FROM
	product_aggregation;
GO