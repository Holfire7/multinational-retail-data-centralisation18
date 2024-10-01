ALTER TABLE dim_products
RENAME COLUMN removed TO still_available;

UPDATE dim_products
SET still_available = CASE
    WHEN still_available = 'Still_avaliable' THEN 'TRUE'
    WHEN still_available = 'Removed' THEN 'FALSE'
END;


ALTER TABLE dim_products
    ALTER COLUMN product_price TYPE FLOAT USING product_price::FLOAT,
    ALTER COLUMN weight TYPE FLOAT USING weight::FLOAT,                
    ALTER COLUMN "EAN" TYPE VARCHAR(255),                               
    ALTER COLUMN product_code TYPE VARCHAR(255),                       
    ALTER COLUMN date_added TYPE DATE USING date_added::DATE,          
    ALTER COLUMN uuid TYPE UUID USING uuid::UUID,                      
    ALTER COLUMN still_available TYPE BOOLEAN USING still_available::BOOLEAN,  
    ALTER COLUMN weight_class TYPE VARCHAR(255); 

SELECT * FROM dim_products