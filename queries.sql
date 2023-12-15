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

---Top 6 months for sales---
SELECT 
	ROUND(SUM(orders_table.product_quantity::numeric * dim_products.product_price::numeric), 2) AS total_sales, 
	EXTRACT('month' from dim_date_times.purchase_date) AS month 
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
	6;

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
	EXTRACT('year' from dim_date_times.purchase_date) as year,
	EXTRACT('month' from dim_date_times.purchase_date) as month
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

---Sales in German stores by type of store---
SELECT
	ROUND(SUM(orders_table.product_quantity::numeric * dim_products.product_price::numeric), 2) AS total_sales,
	dim_store_details.store_type, 
	dim_store_details.country AS country_code
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
WHERE
	dim_store_details.country = 'DE'
GROUP BY
	dim_store_details.store_type,
	dim_store_details.country
ORDER BY
	total_sales;
	
---Average difference in time between sales by year---
WITH times AS 
(
	SELECT
		DISTINCT EXTRACT(year FROM purchase_date) as year,
		LEAD(purchase_time) OVER (PARTITION BY EXTRACT(year FROM purchase_date) ORDER BY purchase_time) - purchase_time AS next_time_stamp
	FROM 
		dim_date_times
	)
SELECT 
	year,
	AVG(next_time_stamp) as actual_time_taken
FROM
	times
GROUP BY  
	DISTINCT times.year			
ORDER BY
	AVG(times.next_time_stamp) DESC;