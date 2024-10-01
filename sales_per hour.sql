WITH sales_time_differences AS (
    SELECT 
        TO_TIMESTAMP(
            CONCAT(
                d.year, '-', 
                LPAD(d.month::text, 2, '0'), '-', 
                LPAD(d.date::text, 2, '0'), ' ', 
                d.timestamp::time
            ), 'YYYY-MM-DD HH24:MI:SS'
        ) AS sale_datetime,
        LEAD(
            TO_TIMESTAMP(
                CONCAT(
                    d.year, '-', 
                    LPAD(d.month::text, 2, '0'), '-', 
                    LPAD(d.date::text, 2, '0'), ' ', 
                    d.timestamp::time
                ), 'YYYY-MM-DD HH24:MI:SS'
            )
        ) OVER (
            ORDER BY 
                TO_TIMESTAMP(
                    CONCAT(
                        d.year, '-', 
                        LPAD(d.month::text, 2, '0'), '-', 
                        LPAD(d.date::text, 2, '0'), ' ', 
                        d.timestamp::time
                    ), 'YYYY-MM-DD HH24:MI:SS'
                )
        ) AS next_sale_datetime,
        d.year
    FROM dim_date_times d
)
SELECT 
    year,
    CONCAT(
        '"hours": ', FLOOR(EXTRACT(EPOCH FROM AVG(next_sale_datetime - sale_datetime)) / 3600), ', ',
        '"minutes": ', FLOOR((EXTRACT(EPOCH FROM AVG(next_sale_datetime - sale_datetime)) % 3600) / 60), ', ',
        '"seconds": ', FLOOR(EXTRACT(EPOCH FROM AVG(next_sale_datetime - sale_datetime)) % 60), ', ',
        '"milliseconds": ', (EXTRACT(EPOCH FROM AVG(next_sale_datetime - sale_datetime)) * 1000) % 1000
    ) AS actual_time_taken
FROM sales_time_differences
WHERE next_sale_datetime IS NOT NULL
GROUP BY year
ORDER BY actual_time_taken DESC;
