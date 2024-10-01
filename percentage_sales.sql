WITH gross_sale AS(
	SELECT SUM(dim_products.product_price * orders_table.product_quantity) AS total_sales
		FROM dim_products
JOIN
	orders_table ON orders_table.product_code = dim_products.product_code
	)


SELECT dim_store_details.store_type,
		SUM(dim_products.product_price * orders_table.product_quantity) AS total_store_sales,
		SUM(dim_products.product_price * orders_table.product_quantity) * 100.0/ (SELECT total_sales FROM gross_sale) AS percentage_total
FROM dim_products
JOIN
	orders_table ON orders_table.product_code = dim_products.product_code
JOIN
	dim_store_details ON dim_store_details.store_code = orders_table.store_code
GROUP BY
		dim_store_details.store_type
ORDER BY 
		total_store_sales DESC;