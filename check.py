import yaml
from sqlalchemy import create_engine, text
import pandas as pd

class DatabaseConnector:
    def read_db_creds(self, filepath='db_creds.yaml'):
        """Reads the database credentials from a YAML file."""
        with open(filepath, 'r') as file:
            creds = yaml.safe_load(file)
        return creds

    def init_db_engine(self):
        """Initializes and returns an SQLAlchemy engine using the credentials."""
        creds = self.read_db_creds()
        engine = create_engine(f"postgresql://{creds['RDS_USER']}:{creds['RDS_PASSWORD']}@"
                               f"{creds['RDS_HOST']}:{creds['RDS_PORT']}/{creds['RDS_DATABASE']}")
        return engine
 
    def list_db_tables(self):
        """Lists all tables in the database."""
        engine = self.init_db_engine()
        with engine.connect() as connection:
            query = text("SELECT table_name FROM information_schema.tables WHERE table_schema='public'")
            result = connection.execute(query)
            tables = [row[0] for row in result]  # Access the first element (table_name)
        return tables

   
import pandas as pd

class DataExtractor:
    def read_rds_table(self, db_connector: DatabaseConnector, table_name: str):
        """Extracts data from a specified RDS table into a Pandas DataFrame."""
        engine = db_connector.init_db_engine()
        query = f"SELECT * FROM {table_name}"
        df = pd.read_sql(query, engine)
        return df




# Initialize objects
db_connector = DatabaseConnector()
data_extractor = DataExtractor()


# Step 1: List the tables to find user data table
tables = db_connector.list_db_tables()
print(tables)  # Identify the correct table for user data, e.g., 'users'

# Step 2: Extract user data
user_data = data_extractor.read_rds_table(db_connector, 'legacy_store_details')
print(user_data)
user_data1 = data_extractor.read_rds_table(db_connector, 'dim_card_details')
print(user_data1)
user_data2 = data_extractor.read_rds_table(db_connector, 'legacy_users',)
print(user_data2)
user_data3 = data_extractor.read_rds_table(db_connector, 'orders_table',)
print(user_data3)




