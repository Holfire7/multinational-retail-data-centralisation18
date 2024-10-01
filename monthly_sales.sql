SELECT SUM(dim_products.product_price * orders_table.product_quantity) AS total_sales, 
	dim_date_times.year,
	dim_date_times.month
			
FROM dim_date_times
JOIN
	orders_table ON orders_table.date_uuid = dim_date_times.date_uuid
JOIN
	dim_products ON dim_products.product_code = orders_table.product_code
GROUP BY
		dim_date_times.month,
		dim_date_times.year
ORDER BY
		total_sales DESC
LIMIT 10;


