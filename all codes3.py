import requests
import yaml
from sqlalchemy import create_engine, inspect
import pandas as pd
import numpy as np
from datetime import datetime
import tabula
import boto3
import io
from io import StringIO
import re
import json
import time


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

class DataExtractor:
    def __init__(self,  api_key: str):
        self.headers = {
            'x-api-key': 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'
        }
        self.s3_client = boto3.client('s3')
        self.api_key = api_key
        

    @staticmethod
    def read_rds_table(connector: DatabaseConnector, table_name: str) -> pd.DataFrame:
        """Reads data from a specified table in the PostgreSQL database."""
        if not hasattr(connector, 'engine') or connector.engine is None:
            connector.init_db_engine()

        query = f"SELECT * FROM {table_name}"
        df = pd.read_sql(query, connector.engine)
        return df

    def retrieve_pdf_data(self, pdf_link: str) -> pd.DataFrame:

        # Extract tables from the PDF (from all pages)
        tables = tabula.read_pdf(pdf_link, pages="all", multiple_tables=True)

        # Combine all tables into a single DataFrame
        if tables:
            combined_df = pd.concat(tables, ignore_index=True)
        else:
            raise ValueError("No tables found in PDF")
        return combined_df
    
    
    def list_number_of_stores(self, number_of_stores_endpoint):
        response = requests.get(number_of_stores_endpoint, headers=self.headers)
        if response.status_code == 200:
            return response.json().get('number_stores', 0)
        return f"Error: {response.status_code}"
        
    def retrieve_stores_data(self, store_details_endpoint):
        # List to store the details of all stores
        stores_data = []

        # Retrieve the total number of stores using the previous method
        number_of_stores_endpoint = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores"
        total_stores = self.list_number_of_stores(number_of_stores_endpoint)

        # Check if total_stores is valid
        if isinstance(total_stores, int):
            # Loop through each store number to retrieve its details
            for store_number in range(0, total_stores):
                # Format the store details endpoint with the store number
                store_url = store_details_endpoint.format(store_number=store_number)
                
                # Make a GET request to retrieve store details
                response = requests.get(store_url, headers=self.headers)
                
                # If the request is successful, append the store data to the list
                if response.status_code == 200:
                    store_data = response.json()
                    stores_data.append(store_data)
                else:
                    print(f"Failed to retrieve data for store {store_number}. Status code: {response.status_code}")

            # Create a DataFrame from the collected store data
            stores_df = pd.DataFrame(stores_data)
            return stores_df
        else:
            # Return a clear error message
            return f"Error: Failed to retrieve the total number of stores: {total_stores}"
        
    def retrieve_store_data_with_retry(self, store_url, retries=3, delay=2):
        """
        Try to retrieve store data with a retry mechanism in case of server errors.
        """
        for attempt in range(retries):
            response = requests.get(store_url, headers=self.headers)
            if response.status_code == 200:
                return response.json()
            elif response.status_code >= 500:
                print(f"Server error for {store_url}. Attempt {attempt + 1} of {retries}. Retrying...")
                time.sleep(delay)  # Wait before retrying
            else:
                print(f"Failed to retrieve data for {store_url}. Status code: {response.status_code}")
                return None
        return None
        
    def extract_from_s3(self, s3_address):
        # Parse the S3 bucket and key from the provided S3 address
        s3_bucket, s3_key = self._parse_s3_address(s3_address)
        
        # Download the object from S3
        obj = self.s3_client.get_object(Bucket=s3_bucket, Key=s3_key)
        
        # Read the CSV data
        data = obj['Body'].read().decode('utf-8')
        
        # Convert the CSV data into a pandas DataFrame
        df = pd.read_csv(StringIO(data))
        
        return df
    
    def _parse_s3_address(self, s3_address):
        # Remove the 's3://' prefix and split the address into bucket and key
        s3_path = s3_address.replace("s3://", "")
        s3_bucket, s3_key = s3_path.split("/", 1)
        return s3_bucket, s3_key

    def extract_json_data(self, json_url):
        # Download the JSON file from the provided S3 link
        response = requests.get(json_url)
        
        if response.status_code == 200:
            # Load the JSON data into a pandas DataFrame
            data = response.json()
            df = pd.DataFrame(data)
            return df
        else:
            return f"Failed to retrieve data. Status code: {response.status_code}"

class DataCleaning:
    def __init__(self):
        pass

    def clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Cleans the DataFrame by handling nulls, converting data types, and deduplication."""
        # 1. Handle NULL values: drop rows that are completely null, fill specific columns
        df.dropna(how='all', inplace=True)  # Remove completely null rows
        df.fillna({'store_name': 'Unknown', 'staff_numbers': 0}, inplace=True)


        # 2. Handle date columns: Ensure valid date formats
        if 'opening_date' in df.columns:
            df['opening_date'] = pd.to_datetime(df['opening_date'], errors='coerce')
            df['opening_date'] = df['opening_date'].fillna(pd.Timestamp('1900-01-01'))  # Set default for invalid dates

        # 3. Deduplicate data if necessary
        df.drop_duplicates(inplace=True)

        # 6. Reset index after cleaning
        df.reset_index(drop=True, inplace=True)

        return df

    def clean_card_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Cleans card data."""
        # Drop rows with any NULL values
        df_cleaned = df.dropna()

        # Remove any duplicate rows
        df_cleaned = df_cleaned.drop_duplicates()

        # Convert a 'Date' column to a proper datetime format (if it exists)
        if 'Date' in df_cleaned.columns:
            df_cleaned['Date'] = pd.to_datetime(df_cleaned['Date'], errors='coerce')

        # Remove rows where a 'Card Number' column has erroneous values (e.g., non-numeric or out of range)
        if 'Card Number' in df_cleaned.columns:
            df_cleaned['Card Number'] = df_cleaned['Card Number'].astype(str)
            df_cleaned = df_cleaned[df_cleaned['Card Number'].str.isdigit()]

        # Removing values that fall out of an expected range (e.g., invalid transaction amounts)
        if 'Transaction Amount' in df_cleaned.columns:
            df_cleaned = df_cleaned[(df_cleaned['Transaction Amount'] > 0) & (df_cleaned['Transaction Amount'] < 10000)]

        # Reset index after cleaning
        df_cleaned = df_cleaned.reset_index(drop=True)

        return df_cleaned

    def clean_store_data(self, df: pd.DataFrame) -> pd.DataFrame:

        # 1. Handle 'opening_date' by converting it to datetime and dealing with invalid dates
        if 'opening_date' in df.columns:
            df['opening_date'] = pd.to_datetime(df['opening_date'], errors='coerce')
            df['opening_date'] = df['opening_date'].fillna(pd.Timestamp('1900-01-01'))  # Default invalid dates

        # 2. Keep only rows with valid 'country_code' values
        valid_country_codes = ['GB', 'US', 'DE']
        if 'country_code' in df.columns:
            df = df[df['country_code'].isin(valid_country_codes)]
        
        # 3. Deduplicate data if necessary
        df.drop_duplicates(inplace=True)

        # 4. Reset the index after cleaning
        df.reset_index(drop=True, inplace=True)

        return df
    
    def clean_address(self, address: str) -> str:
        """Cleans the address field."""
        address = address.replace('\n', ' ')
        address = address.strip()
        return address

    def convert_product_weights(self, df):
        # Define conversion factors
        g_to_kg = 0.001
        lb_to_kg = 0.453592
        oz_to_kg = 0.0283495
        ml_to_g = 1  # 1:1 ratio for ml to g, as rough estimate
        
        def convert_weight(value):
            value = str(value).lower()  # Convert to lowercase for uniformity
            
            # Remove any excess characters
            value = re.sub(r'[^\d\.mlkg]', '', value)
            
            if 'kg' in value:
                # Direct conversion to float
                return float(re.sub(r'[^\d\.]', '', value))
            
            elif 'g' in value:
                # Convert grams to kilograms
                return float(re.sub(r'[^\d\.]', '', value)) * g_to_kg
            
            elif 'lb' in value:
                # Convert pounds to kilograms
                return float(re.sub(r'[^\d\.]', '', value)) * lb_to_kg
            
            elif 'oz' in value:
                # Convert ounces to kilograms
                return float(re.sub(r'[^\d\.]', '', value)) * oz_to_kg
            
            elif 'ml' in value:
                # Treat milliliters as grams and convert to kilograms
                return float(re.sub(r'[^\d\.]', '', value)) * ml_to_g * g_to_kg
            
            else:
                # If no unit specified, return NaN or 0 as a default value
                return float(value) if value.isnumeric() else None
        
        # Apply the conversion to the 'weight' column
        df['weight'] = df['weight'].apply(convert_weight)
        
        return df
    
    def clean_products_data(self, df):
        # Step 1: Convert product weights to kilograms
        df = self.convert_product_weights(df)

        # Step 2: Convert the 'weight' column to numeric, forcing invalid values to NaN
        df['weight'] = pd.to_numeric(df['weight'], errors='coerce')

        # Step 3: Drop rows with missing or invalid weight values (NaN)
        df = df.dropna(subset=['weight'])

        # Step 4: Remove any rows with negative or zero weights (assuming these are erroneous)
        df = df[df['weight'] > 0]

         # Step 5: Clean 'product_price' by removing '£' and converting to numeric
        df['product_price'] = df['product_price'].str.replace("£", "").str.strip()  # Remove '£' and trim spaces
        df['product_price'] = pd.to_numeric(df['product_price'], errors='coerce')  # Convert to numeric, invalid values to NaN

        # Step 6: Drop rows with missing or invalid price values (NaN)
        df = df.dropna(subset=['product_price'])

        # Step 7: Remove duplicate rows (if applicable)
        df = df.drop_duplicates()

        # Step 8: Trim whitespace from all string columns (if any)
        df = df.apply(lambda col: col.str.strip().replace("£", " ") if col.dtype == "object" else col)

        # Step 9: Handle any additional domain-specific cleaning logic (e.g., correcting known issues)
        df = df.dropna(subset=['product_name'])

        # Step 10: Reset the index after cleaning
        df = df.reset_index(drop=True)

        return df
    
    def clean_orders_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Cleans the orders data by removing unnecessary columns."""
        # Drop the columns: 'first_name', 'last_name', and '1'
        columns_to_remove = ['first_name', 'last_name', '1']
        df = df.drop(columns=[col for col in columns_to_remove if col in df.columns], errors='ignore')
        
        # Reset the index after cleaning
        df.reset_index(drop=True, inplace=True)
        
        return df
    
    @staticmethod
    def clean_date_data(df: pd.DataFrame) -> pd.DataFrame:
        """Cleans the date_times data."""
        # Convert timestamp to datetime
        df['timestamp'] = pd.to_datetime(df['timestamp'], format='%H:%M:%S', errors='coerce')
    
        # Convert date columns to numeric
        date_columns = ['year', 'month', 'day']
        for col in date_columns:
             df[col] = pd.to_numeric(df[col], errors='coerce')
    
        # Create a date column
        df['date'] = pd.to_datetime(df[['year', 'month', 'day']], errors='coerce')
    
        # Drop rows with invalid dates
        df = df.dropna(subset=['date', 'timestamp'])
    
        # Reset index after cleaning
        df = df.reset_index(drop=True)
    
        return df

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

if __name__ == "__main__":
    # Initialize connectors and extractors
    rds_connector = DatabaseConnector('db_creds.yaml')
    api_key = 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'
    data_extractor = DataExtractor(api_key)
    data_cleaner = DataCleaning()  # Instantiate the DataCleaning class

    # Process user data
    user_data = data_extractor.read_rds_table(rds_connector, "legacy_store_details")
    #user_data1 = data_extractor.read_rds_table(rds_connector, 'dim_card_details')
    user_data2 = data_extractor.read_rds_table(rds_connector, 'legacy_users',)
    #user_data3 = data_extractor.read_rds_table(rds_connector, 'orders_table',)
    cleaned_user_data = data_cleaner.clean_data(user_data)  # Pass the user data to clean_data
    #cleaned_user_data = data_cleaner.clean_data(user_data1)
    cleaned_user_data2 = data_cleaner.clean_data(user_data2)
    #cleaned_user_data = data_cleaner.clean_data(user_data3)

    # Process card data
    pdf_url = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf'
    card_data = data_extractor.retrieve_pdf_data(pdf_url)
    cleaned_card_data = data_cleaner.clean_card_data(card_data)

    # Process store data
    number_of_stores_endpoint = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores"
    store_details_endpoint = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store_number}"
    number_of_stores = data_extractor.list_number_of_stores(number_of_stores_endpoint)
    print(number_of_stores)
    stores_df =  data_extractor.retrieve_stores_data(store_details_endpoint)
    print(stores_df)
    cleaned_stores_data = data_cleaner.clean_store_data(stores_df)

    # Process product data
    products_s3_url = 's3://data-handling-public/products.csv'
    products_data = data_extractor.extract_from_s3(products_s3_url)
    cleaned_products_data = data_cleaner.clean_products_data(products_data)

    # Process orders data
    orders_data = data_extractor.read_rds_table(rds_connector, "orders_table")
    cleaned_orders_data = data_cleaner.clean_orders_data(orders_data)

    # Process date times data
    json_url = "https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json"
    date_df = data_extractor.extract_json_data(json_url)
    cleaned_date_data = data_cleaner.clean_date_data(date_df)

    # Upload cleaned data to the database
    local_connector = LocalPostgresConnector('local_db_creds.yaml')
    local_connector.upload_dataframe(cleaned_user_data, 'dim_users')
    local_connector.upload_dataframe(cleaned_user_data2, 'dim_users')
    local_connector.upload_dataframe(cleaned_card_data, 'dim_card_details')
    local_connector.upload_dataframe(cleaned_stores_data, 'dim_store_details')
    local_connector.upload_dataframe(cleaned_products_data, 'dim_products')
    local_connector.upload_dataframe(cleaned_orders_data, 'orders_table')
    local_connector.upload_dataframe(cleaned_date_data, 'dim_date_times')

    print("All data has been processed and uploaded to the local PostgreSQL database.")
