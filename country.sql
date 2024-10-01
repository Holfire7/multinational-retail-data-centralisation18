SELECT country_code, COUNT(*) AS total_stores FROM dim_store_details
GROUP BY country_code
ORDER BY total_stores DESC;