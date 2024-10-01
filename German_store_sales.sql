SELECT SUM(dim_products.product_price * orders_table.product_quantity) AS total_sales,
	dim_store_details.store_type, 
	dim_store_details.country_code
FROM
	dim_store_details
JOIN
	orders_table ON dim_store_details.store_code = orders_table.store_code
JOIN
	dim_products ON orders_table.product_code = dim_products.product_code
WHERE
	country_code = 'DE'
GROUP BY
		dim_store_details.store_type,
		dim_store_details.country_code
ORDER  BY
		total_sales ASC;
		 