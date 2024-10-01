DELETE FROM dim_users
WHERE user_uuid::text !~ '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$';

ALTER TABLE dim_users
            ALTER COLUMN user_uuid TYPE UUID USING user_uuid::uuid,
            ALTER COLUMN first_name TYPE VARCHAR (255),
            ALTER COLUMN last_name TYPE VARCHAR(255),
            ALTER COLUMN country_code TYPE VARCHAR(255),
            ALTER COLUMN date_of_birth TYPE DATE USING date_of_birth::date,
            ALTER COLUMN join_date TYPE DATE USING join_date::date;