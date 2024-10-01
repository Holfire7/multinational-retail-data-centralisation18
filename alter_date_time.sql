 ALTER TABLE dim_date_times
        ALTER COLUMN month TYPE VARCHAR(50),
        ALTER COLUMN year TYPE VARCHAR(50),
        ALTER COLUMN day TYPE VARCHAR(50),
        ALTER COLUMN time_period TYPE VARCHAR(50),
        ALTER COLUMN date_uuid TYPE UUID USING date_uuid::uuid

SELECT * FROM dim_date_times