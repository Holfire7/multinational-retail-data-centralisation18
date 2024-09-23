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


alter_date_column = text('''
        ALTER TABLE dim_date_times
        ALTER COLUMN month TYPE VARCHAR(50),
        ALTER COLUMN year TYPE VARCHAR(50),
        ALTER COLUMN day TYPE VARCHAR(50),
        ALTER COLUMN time_period TYPE VARCHAR(50),
        ALTER COLUMN date_uuid TYPE UUID USING date_uuid::uuid
''')

with engine.connect () as con:
    con.execute(alter_date_column)
    con.commit()
    print("Table altered successfully")