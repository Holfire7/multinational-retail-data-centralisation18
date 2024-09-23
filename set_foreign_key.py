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


alter_columns = text('''
    ALTER TABLE orders_table
    ALTER COLUMN date_uuid TYPE UUID USING date_uuid::uuid;
                       
    ALTER TABLE orders_table
    ALTER COLUMN card_number TYPE VARCHAR(50);
''')

insert_missing_card_numbers = text('''
    INSERT INTO dim_card_details (card_number)
    SELECT DISTINCT card_number
    FROM orders_table
    WHERE card_number IS NOT NULL
    AND card_number NOT IN (SELECT card_number FROM dim_card_details);
''')

set_foreign = text('''              
              
    ALTER TABLE orders_table
    ADD CONSTRAINT fk_date_uuid FOREIGN KEY (date_uuid) REFERENCES dim_date_times(date_uuid);
    
    ALTER TABLE orders_table
    ADD CONSTRAINT fk_user_uuid FOREIGN KEY (user_uuid) REFERENCES dim_users(user_uuid);
    
    ALTER TABLE orders_table
    ADD CONSTRAINT fk_card_number FOREIGN KEY (card_number) REFERENCES dim_card_details(card_number);
    
    ALTER TABLE orders_table
    ADD CONSTRAINT fk_store_code FOREIGN KEY (store_code) REFERENCES dim_store_details(store_code);
    
    ALTER TABLE orders_table
    ADD CONSTRAINT fk_product_code FOREIGN KEY (product_code) REFERENCES dim_products(product_code);
''')

with engine.connect() as con:
    con.execute(alter_columns)
    con.execute(insert_missing_card_numbers)
    con.execute(set_foreign)

print("Foreign keys set")
