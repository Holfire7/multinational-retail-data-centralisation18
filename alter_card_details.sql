
UPDATE dim_card_details
SET date_payment_confirmed = NULL
WHERE date_payment_confirmed = 'NULL'
   OR date_payment_confirmed !~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'; 


ALTER TABLE dim_card_details
    ALTER COLUMN card_number TYPE VARCHAR(50),
    ALTER COLUMN expiry_date TYPE VARCHAR(50),
    ALTER COLUMN date_payment_confirmed TYPE DATE USING NULLIF(date_payment_confirmed, '')::DATE;


SELECT * FROM dim_card_details