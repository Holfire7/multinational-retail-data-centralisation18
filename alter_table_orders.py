from sqlalchemy import create_engine, text
import yaml


with open('local_db_creds.yaml', 'r') as file:
    config = yaml.safe_load(file)


database_type = config['DATABASE_TYPE']  
dbapi = config['DBAPI'] 
host = config['HOST']
user = config['USER']
password = config['PASSWORD']
database = config['DATABASE']
port = config['PORT']

engine = create_engine(f"{database_type}+{dbapi}://{user}:{password}@{host}:{port}/{database}", echo=True)

alter_d_type_column = text("""
        ALTER TABLE orders_table
            ALTER COLUMN date_uuid TYPE UUID USING date_uuid::uuid,
            ALTER COLUMN user_uuid TYPE UUID USING user_uuid::uuid,
            ALTER COLUMN card_number TYPE VARCHAR (50),
            ALTER COLUMN store_code TYPE VARCHAR(50),
            ALTER COLUMN product_code TYPE VARCHAR(50),
            ALTER COLUMN product_quantity TYPE SMALLINT;
            """)

with engine.connect() as con:
    con.execute(alter_d_type_column)
    con.commit()

print('Table columns altered successfully')