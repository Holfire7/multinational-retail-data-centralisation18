WITH time_difference AS (
    SELECT 
        "year",
        "timestamp" - LEAD("timestamp") OVER (PARTITION BY "year" ORDER BY "timestamp") AS time_between_sales
    FROM 
        dim_date_times
)
SELECT 
    "year",
    AVG(time_between_sales) AS actual_time_taken
FROM 
    time_difference
WHERE 
    time_between_sales IS NOT NULL
GROUP BY 
    "year"
ORDER BY 
    actual_time_taken;
