from sqlalchemy import create_engine, text
import yaml
import uuid

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
engine = create_engine(
    f"{database_type}+{dbapi}://{user}:{password}@{host}:{port}/{database}",
    echo=True
)

def set_prim_key(table_name, column_name):
    drop_pri_key = text(f'''
        ALTER TABLE {table_name}
        DROP CONSTRAINT IF EXISTS {table_name}_pkey  
    ''')
    set_not_null = text(f'''
        ALTER TABLE {table_name}
        ALTER COLUMN {column_name} SET NOT NULL
    ''')
    add_prim_key = text(f'''
        ALTER TABLE {table_name}
        ADD PRIMARY KEY ({column_name})
    ''')

    with engine.begin() as con:
        con.execute(drop_pri_key)
        con.execute(set_not_null)
        con.execute(add_prim_key)
        print(f'Primary Key set on {table_name}({column_name})')

# Handle NULL store_code values
with engine.begin() as con:
    # Assign new unique store_code values
    con.execute(text('''
        UPDATE dim_store_details
        SET store_code = :new_code
        WHERE store_code IS NULL OR TRIM(store_code) = '' OR LOWER(store_code) = 'null'
    '''), {'new_code': str(uuid.uuid4())})
    print("Assigned new store_code values to rows where store_code was NULL or invalid.")

# Identify and resolve duplicate store_code values
with engine.connect() as con:
    duplicates = con.execute(text('''
        SELECT store_code, COUNT(*) FROM dim_store_details
        GROUP BY store_code
        HAVING COUNT(*) > 1
    ''')).fetchall()

if duplicates:
    print("Duplicate store_code values found:")
    for store_code, count in duplicates:
        print(f"store_code: {store_code}, Count: {count}")
    # Resolve duplicates
    with engine.begin() as con:
        for store_code, count in duplicates:
            # Fetch all but one row for each duplicate store_code
            rows = con.execute(text('''
                SELECT ctid FROM dim_store_details WHERE store_code = :store_code
            '''), {'store_code': store_code}).fetchall()

            for row in rows[1:]:
                ctid = row[0]
                new_code = store_code + '_' + str(uuid.uuid4())[:8]
                con.execute(text('''
                    UPDATE dim_store_details
                    SET store_code = :new_code
                    WHERE ctid = :ctid
                '''), {'new_code': new_code, 'ctid': ctid})
        print("Updated duplicate store_code values to unique ones.")
else:
    print("No duplicate store_code values found.")

# Verify data before setting primary key
with engine.connect() as con:
    null_count = con.execute(text('''
        SELECT COUNT(*) FROM dim_store_details WHERE store_code IS NULL
    ''')).scalar()
    duplicate_count = con.execute(text('''
        SELECT COUNT(*) FROM (
            SELECT store_code FROM dim_store_details
            GROUP BY store_code
            HAVING COUNT(*) > 1
        ) sub
    ''')).scalar()

print(f"Number of NULL store_code values: {null_count}")
print(f"Number of duplicate store_code values: {duplicate_count}")

if null_count == 0 and duplicate_count == 0:
    # Set primary key
    set_prim_key('dim_store_details', 'store_code')
else:
    print("Cannot set primary key due to NULL or duplicate values in store_code.")

set_prim_key('dim_date_times', 'date_uuid')
set_prim_key('dim_users', 'user_uuid')
set_prim_key('dim_card_details', 'card_number')
set_prim_key('dim_products', 'product_code')
print('Primary keys set')