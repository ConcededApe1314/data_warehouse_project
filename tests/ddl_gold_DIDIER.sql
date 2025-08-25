-- Elimina la vista si ya existe para evitar errores al volver a ejecutar el script.
DROP VIEW IF EXISTS gold.dim_customers;
GO

CREATE VIEW gold.dim_customers AS 
SELECT
    ROW_NUMBER() OVER (ORDER BY ci.cst_id) AS customer_key,
    ci.cst_id AS customer_id,
    ci.cst_key AS customer_number,
    ci.cst_firstname AS first_name,
    ci.cst_lastname AS last_name,
    cl.cntry AS country,
    ci.cst_marital_status AS marital_status,
    CASE
        WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr
        ELSE COALESCE(ce.gen, 'n/a')
    END AS gender,
    ce.bdate AS birthdate,
    ci.cst_create_date AS create_date
FROM
    silver.crm_cust_info AS ci
LEFT JOIN
    silver.erp_cust_az12 AS ce
ON
    ci.cst_key = ce.cid
LEFT JOIN
    silver.erp_loc_a101 AS cl
ON
    ci.cst_key = cl.cid;
GO


-- Elimina la vista si ya existe para evitar errores al volver a ejecutar el script.
DROP VIEW IF EXISTS gold.dim_products;
GO

CREATE VIEW gold.dim_products AS 
SELECT
    ROW_NUMBER() OVER (ORDER BY po.prd_start_dt, po.prd_sls_key) AS product_key,
    po.prd_id AS product_id,
    po.prd_sls_key AS product_number,
    po.prd_nm AS product_name,
    po.prd_cat_id AS category_id,
    pc.cat AS category,
    pc.subcat AS subcategory,
    pc.maintenance,
    po.prd_cost AS cost,
    po.prd_line AS product_line,
    po.prd_start_dt AS start_date 
FROM
    silver.crm_prd_info AS po
LEFT JOIN
    silver.erp_px_cat_g1v2 AS pc
ON
    po.prd_cat_id = pc.id
WHERE
    po.prd_end_dt IS NULL;
GO


-- Elimina la vista si ya existe para evitar errores al volver a ejecutar el script.
DROP VIEW IF EXISTS gold.fact_sales;
GO

CREATE VIEW gold.fact_sales AS
SELECT
    sd.sls_ord_num AS order_number,
    pr.product_key,
    cs.customer_key,
    sd.sls_order_dt AS order_date,
    sd.sls_ship_dt AS shipping_date,
    sd.sls_due_dt AS due_date,
    sd.sls_sales AS sales_amount,
    sd.sls_quantity AS quantity,
    sd.sls_price AS price
FROM
    silver.crm_sales_details AS sd
LEFT JOIN
    gold.dim_products AS pr
ON
    sd.sls_prd_key = pr.product_number
LEFT JOIN
    gold.dim_customers AS cs
ON
    sd.sls_cust_id = cs.customer_id;
GO