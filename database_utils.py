import yaml
from sqlalchemy import create_engine, inspect
import pandas as pd

class DatabaseConnector:
    def __init__(self, db_creds: str):
        self.db_creds = db_creds
        self.engine = None  # Initialize engine as None

    def read_db_creds(self) -> dict:
        """Reads database credentials from a YAML file."""
        with open(self.db_creds, 'r') as file:
            db_creds = yaml.safe_load(file)
        return db_creds

    def init_db_engine(self):
        """Initializes the database engine using credentials from the YAML file."""
        creds = self.read_db_creds()
        db_type = creds.get('db_type', 'postgresql')
        db_api = creds.get('db_api', 'psycopg2')
        host = creds['RDS_HOST']
        user = creds['RDS_USER']
        password = creds['RDS_PASSWORD']
        database = creds['RDS_DATABASE']
        port = creds.get('RDS_PORT', 5432)  # Default to 5432 if PORT is not provided

        connector = f"{db_type}+{db_api}://{user}:{password}@{host}:{port}/{database}"
        self.engine = create_engine(connector)
        return self.engine

    def list_db_tables(self):
        """Lists all tables available in the PostgreSQL database."""
        if self.engine is None:
            self.init_db_engine()

        inspector = inspect(self.engine)
        return inspector.get_table_names()

    def upload_to_db(self, df: pd.DataFrame, table_name: str, if_exists: str = 'replace'):
        """Uploads a DataFrame to a specified table in the PostgreSQL database."""
        if self.engine is None:
            self.init_db_engine()

        try:
            df.to_sql(table_name, con=self.engine, if_exists=if_exists, index=False)
            print(f"Data successfully uploaded to table '{table_name}'.")
        except Exception as e:
            print(f"Error uploading data to table '{table_name}': {e}")

    
class LocalPostgresConnector:
    def __init__(self, local_db_creds: str):
        self.local_db_creds = local_db_creds
        self.engine = None

    def read_local_db_creds(self) -> dict:
        """Reads local database credentials from a YAML file."""
        with open(self.local_db_creds, 'r') as file:
            local_db_creds = yaml.safe_load(file)
        return local_db_creds

    def initialize_engine(self):
        """Initializes the local PostgreSQL database engine using provided credentials."""
        cred1 = self.read_local_db_creds()
        db_type = cred1.get('DATABASE_TYPE', 'postgresql')
        db_api = cred1.get('DBAPI', 'psycopg2')
        user = cred1.get('USER')
        password = cred1.get('PASSWORD')
        host = cred1.get('HOST')
        database = cred1.get('DATABASE')
        port = cred1.get('PORT', 5432)  

        # Build connection string
        connection_str = f"{db_type}+{db_api}://{user}:{password}@{host}:{port}/{database}"
        self.engine = create_engine(connection_str)
        return self.engine

    def upload_dataframe(self, df: pd.DataFrame, table_name: str, if_exists: str = 'replace'):
        """Uploads a DataFrame to a specified table in the PostgreSQL database."""
        if self.engine is None:
            self.initialize_engine()

        try:
            df.to_sql(table_name, con=self.engine, if_exists=if_exists, index=False)
            print(f"Data successfully uploaded to table '{table_name}'.")
        except Exception as e:
            print(f"Error uploading data to table '{table_name}': {e}")