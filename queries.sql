---Number of stores by region---
SELECT 
	country, 
	COUNT(*) AS total_no_stores
FROM 
	dim_store_details
GROUP BY 
	country
ORDER BY 
	COUNT(*) DESC;

---Number of stores by Locality---
SELECT 
	locality, 
	COUNT(*) AS total_no_stores 
FROM 
	dim_store_details
GROUP BY 
	locality
ORDER BY 
	COUNT(*) DESC
LIMIT 
	7;

---Total sales by month???--- distinct???
WITH values AS (
	SELECT orders_table.product_quantity * dim_products.product_price 
	AS order_values, 
	orders_table.date_uuid AS date
	FROM orders_table
	JOIN dim_products
	ON orders_table.product_code = dim_products.product_code
	)
SELECT SUM(values.order_values) AS total_sales, 
EXTRACT('month' from dim_date_times.purchase_datetime) AS month 
FROM values
JOIN dim_date_times
ON values.date = dim_date_times.date_uuid
GROUP BY month
ORDER BY total_sales DESC
LIMIT 7;

select * from orders_table

---Alternatively:

SELECT 
	SUM(orders_table.product_quantity * dim_products.product_price) AS total_sales, 
	EXTRACT('month' from dim_date_times.purchase_datetime) AS month 
FROM 
	orders_table
JOIN 
	dim_products
ON 
	orders_table.product_code = dim_products.product_code
JOIN 
	dim_date_times
ON 
	orders_table.date_uuid = dim_date_times.date_uuid
GROUP BY 
	month
ORDER BY 
	total_sales DESC
LIMIT 
	7;

---Sales counts and products counts by online and physical ---
WITH online AS 
(
	SELECT 
		COUNT(*) AS number_of_sales,
		SUM(product_quantity) AS product_quantity_count
	FROM 
		orders_table
	JOIN 
		dim_store_details
	ON 
		orders_table.store_code = dim_store_details.store_code
	WHERE 
		store_type = 'Web Portal'
	), 
		offline AS 
		(
			SELECT 
				COUNT(*) AS number_of_sales,
				SUM(product_quantity) AS product_quantity_count
			FROM 
				orders_table
			JOIN 
				dim_store_details
			ON 
				orders_table.store_code = dim_store_details.store_code
			WHERE 
				store_type != 'Web Portal'
		)
	SELECT 
		number_of_sales,
		product_quantity_count,
		'Web' as location
	FROM 
		online
	UNION ALL 
	SELECT
		number_of_sales,
		product_quantity_count,
		'Offline' as location
	FROM 
		offline;

--- Online/offline total sales and items sold ---
SELECT 
	dim_store_details.store_type,
	ROUND(SUM(orders_table.product_quantity::numeric * dim_products.product_price::numeric), 2) AS total_sales,
	ROUND((SUM(orders_table.product_quantity::numeric * dim_products.product_price::numeric) * 100) /
		(
			SELECT 
				SUM(orders_table.product_quantity::numeric * dim_products.product_price::numeric) 
			FROM 
				orders_table
			JOIN 
				dim_products
			ON 
				orders_table.product_code = dim_products.product_code
		), 2) AS percentage 
FROM 
	orders_table
JOIN 
	dim_products
ON 
	orders_table.product_code = dim_products.product_code
JOIN
	dim_store_details
ON
	orders_table.store_code = dim_store_details.store_code
GROUP BY 
	dim_store_details.store_type
ORDER BY 
	total_sales DESC;
	
--- Top total sales by year and month ---
SELECT
	ROUND(SUM(orders_table.product_quantity::numeric * dim_products.product_price::numeric), 2) AS total_sales,
	EXTRACT('year' from dim_date_times.purchase_datetime) as year,
	EXTRACT('month' from dim_date_times.purchase_datetime) as month
FROM
	orders_table
JOIN 
	dim_products
ON 
	orders_table.product_code = dim_products.product_code
JOIN 
	dim_date_times
ON
	orders_table.date_uuid = dim_date_times.date_uuid
GROUP BY 
	month,
	year
ORDER BY 
	total_sales desc
LIMIT 
	10;

--- Total staff numbers by country ---
SELECT 
	SUM(staff_numbers) AS total_staff_numbers,
	country
FROM 
	dim_store_details
GROUP BY 
	country
ORDER BY 
	total_staff_numbers DESC;






