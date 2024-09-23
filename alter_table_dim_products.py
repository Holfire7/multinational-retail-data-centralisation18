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

alter_products_column = text('''
        ALTER TABLE dim_products
        RENAME COLUMN removed TO still_available                          
    ''')

clean_still_available_column = text('''
    UPDATE dim_products
    SET still_available = CASE
        WHEN still_available ILIKE 'Still_avaliable' THEN 'TRUE'
        WHEN still_available ILIKE 'Unavailable' THEN 'FALSE'
        WHEN still_available ILIKE 'Removed' THEN 'FALSE'
        WHEN still_available ILIKE 'Not Removed' THEN 'TRUE'
        ELSE 'FALSE'  -- Default to FALSE for any other unrecognized value
    END
''')

clean_product_price_column = text('''
    UPDATE dim_products
    SET product_price = NULL
    WHERE product_price !~ '^\\d+\\.?\\d*$'  -- This checks if product_price contains non-numeric values
''')

clean_date_added_column = text('''
    UPDATE dim_products
    SET date_added = NULL
    WHERE date_added !~ '^\d{4}-\d{2}-\d{2}$'  -- Only keep values in 'YYYY-MM-DD' format
''')

clean_uuid_column = text('''
    UPDATE dim_products
    SET uuid = NULL
    WHERE uuid !~ '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$'
''')

alter_products_table = text('''
        ALTER TABLE dim_products
        ALTER COLUMN product_price TYPE FLOAT USING product_price::double precision,
        ALTER COLUMN weight TYPE FLOAT,
        ALTER COLUMN "EAN" TYPE VARCHAR(50),
        ALTER COLUMN product_code TYPE VARCHAR(50),
        ALTER COLUMN date_added TYPE DATE USING date_added::date,
        ALTER COLUMN uuid TYPE UUID USING uuid::uuid,
        ALTER COLUMN still_available TYPE BOOL USING still_available::boolean,
        ALTER COLUMN weight_class TYPE VARCHAR(50)
''')

with engine.connect() as con:
    con.execute(alter_products_column)
    con.execute(clean_still_available_column)
    con.execute(clean_product_price_column)
    con.execute(clean_date_added_column) 
    con.execute(clean_uuid_column)
    con.execute(alter_products_table)
    con.commit()
    print('Table columns altered successfully.')
