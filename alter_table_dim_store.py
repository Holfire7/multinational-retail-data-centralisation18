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

# SQL statement to delete rows with invalid longitude, latitude, or staff_numbers
delete_invalid_data = text('''
    DELETE FROM dim_store_details
    WHERE staff_numbers !~ '^[0-9]+$'  -- Delete rows where staff_numbers is not a valid number
    OR CAST(longitude AS TEXT) !~ '^[0-9.\-]+$'  -- Check for valid numeric format in longitude
    OR CAST(latitude AS TEXT) !~ '^[0-9.\-]+$';  -- Check for valid numeric format in latitude
''')

# SQL statement to alter data types of multiple columns
alter_d_type_column = text('''
    ALTER TABLE dim_store_details
    ALTER COLUMN longitude TYPE FLOAT USING longitude::double precision,
    ALTER COLUMN latitude TYPE FLOAT USING latitude::double precision,
    ALTER COLUMN locality TYPE VARCHAR(255),
    ALTER COLUMN store_code TYPE VARCHAR(50),
    ALTER COLUMN country_code TYPE VARCHAR(255),
    ALTER COLUMN staff_numbers TYPE SMALLINT USING NULLIF(staff_numbers, 'J78')::smallint,
    ALTER COLUMN store_type TYPE VARCHAR(255),
    ALTER COLUMN continent TYPE VARCHAR(255),
    ALTER COLUMN opening_date TYPE DATE USING opening_date::date;
''')

# Execute the SQL queries
with engine.connect() as con:
    con.execute(delete_invalid_data)
    con.commit()

   
    con.execute(alter_d_type_column)
    con.commit()

print('Table columns altered successfully.')
