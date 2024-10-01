
UPDATE dim_store_details
SET lat = CASE
    WHEN lat IS NULL OR lat = 'N/A' THEN latitude
    ELSE lat
END;


ALTER TABLE dim_store_details
DROP COLUMN latitude;


UPDATE dim_store_details
SET lat = NULL
WHERE lat = 'N/A' OR lat ~ '[^0-9.-]';

UPDATE dim_store_details
SET longitude = NULL
WHERE longitude = 'N/A' OR longitude ~ '[^0-9.-]';

UPDATE dim_store_details
SET staff_numbers = NULL
WHERE staff_numbers ~ '[^0-9]';

ALTER TABLE store_details_table
    ALTER COLUMN longitude TYPE FLOAT USING longitude::FLOAT,  
    ALTER COLUMN locality TYPE VARCHAR(255),                   
    ALTER COLUMN store_code TYPE VARCHAR(255),                 
    ALTER COLUMN staff_numbers TYPE SMALLINT USING staff_numbers::SMALLINT,  
    ALTER COLUMN opening_date TYPE DATE USING opening_date::DATE,  
    ALTER COLUMN store_type TYPE VARCHAR(255),                  
    ALTER COLUMN latitude_1 TYPE FLOAT USING latitude_1::FLOAT, 
    ALTER COLUMN country_code TYPE VARCHAR(255),                 
    ALTER COLUMN continent TYPE VARCHAR(255);


UPDATE dim_store_details
SET locality = NULL
WHERE locality = 'N/A';



SELECT* from dim_store_details