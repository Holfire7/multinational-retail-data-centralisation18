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

delete_invalid_uuids = text('''
    DELETE FROM dim_users
    WHERE user_uuid !~ '^[0-9a-fA-F-]{8}-[0-9a-fA-F-]{4}-[0-9a-fA-F-]{4}-[0-9a-fA-F-]{4}-[0-9a-fA-F-]{12}$';  -- Regular expression to match valid UUID
''')

alter_d_type_column = text("""
        ALTER TABLE dim_users
            ALTER COLUMN user_uuid TYPE UUID USING user_uuid::uuid,
            ALTER COLUMN first_name TYPE VARCHAR (255),
            ALTER COLUMN last_name TYPE VARCHAR(255),
            ALTER COLUMN country_code TYPE VARCHAR(255),
            ALTER COLUMN date_of_birth TYPE DATE USING date_of_birth::date,
            ALTER COLUMN join_date TYPE DATE USING join_date::date;
                           
            """)

with engine.connect() as con:
    # First, delete rows with invalid UUIDs
    con.execute(delete_invalid_uuids)
    con.commit()

    # Then, proceed with altering the table
    con.execute(alter_d_type_column)
    con.commit()

print('Table columns altered successfully')