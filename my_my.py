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

with engine.connect() as con:
    result = con.execute(text("""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = 'dim_products'
    """))

    for row in result:
        print(row)
