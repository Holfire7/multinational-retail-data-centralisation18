from sqlalchemy import create_engine, text
import yaml

# Load the YAML file
with open('local_db_creds.yaml', 'r') as file:
    config = yaml.safe_load(file)

# Extract database credentials
database_type = config['DATABASE_TYPE']  
dbapi = config['DBAPI'] 
host = config['HOST']
user = config['USER']
password = config['PASSWORD']
database = config['DATABASE']
port = config['PORT']

# Create SQLAlchemy engine
engine = create_engine(f"{database_type}+{dbapi}://{user}:{password}@{host}:{port}/{database}", echo=True)

clean_null_strings = text('''
    UPDATE dim_card_details
    SET date_payment_confirmed = NULL
    WHERE
        TRIM(LOWER(date_payment_confirmed)) IN ('null', '')
        OR date_payment_confirmed IS NULL
''')

convert_dates = text('''
    UPDATE dim_card_details
    SET date_payment_confirmed = TO_CHAR(
        TO_DATE(TRIM(date_payment_confirmed), 'DD/MM/YYYY'),
        'YYYY-MM-DD'
    )
    WHERE
        TRIM(date_payment_confirmed) ~ '^\\d{2}/\\d{2}/\\d{4}$'
''')

trim_dates = text('''
    UPDATE dim_card_details
    SET date_payment_confirmed = TRIM(date_payment_confirmed)
    WHERE date_payment_confirmed IS NOT NULL
''')

clean_invalid_dates = text('''
    UPDATE dim_card_details
    SET date_payment_confirmed = NULL
    WHERE
        date_payment_confirmed IS NOT NULL
        AND (
            TRIM(LOWER(date_payment_confirmed)) IN ('null', 'n/a', 'na')
            OR date_payment_confirmed ~ '[^0-9-]'
            OR LENGTH(TRIM(date_payment_confirmed)) < 10
        )
''')

alter_card_table = text('''
        ALTER TABLE dim_card_details
        ALTER COLUMN card_number TYPE VARCHAR(50),
        ALTER COLUMN expiry_date TYPE VARCHAR(50),
        ALTER COLUMN date_payment_confirmed TYPE DATE USING date_payment_confirmed::date
''')

with engine.connect() as con:
    con.execute(clean_null_strings)
    con.execute(convert_dates)
    con.execute(trim_dates)
    con.execute(clean_invalid_dates)
    con.execute(alter_card_table)
    con.commit()

    print("Table successfully altered")