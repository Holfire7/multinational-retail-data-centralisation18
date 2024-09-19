import pandas as pd
from database_utils import DatabaseConnector
import requests
import tabula
import boto3
import time
import io
from io import StringIO


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
        
    def extract_from_s3(self, s3_url: str) -> pd.DataFrame:
        """Extracts data from an S3 URL and returns a pandas DataFrame."""
        bucket_name, object_key = self._parse_s3_url(s3_url)
        
        # Retrieve the object from S3
        response = self.s3_client.get_object(Bucket=bucket_name, Key=object_key)
        
        # Read the content of the object
        data = response['Body'].read().decode('utf-8')
        
        # Determine the file type based on the object key
        if object_key.endswith('.csv'):
            df = pd.read_csv(StringIO(data))
        elif object_key.endswith('.json'):
            df = pd.read_json(StringIO(data))
        else:
            raise ValueError(f"Unsupported file type: {object_key}")
        
        return df

    def _parse_s3_url(self, s3_url: str) -> tuple:
        """Parses an S3 URL into bucket name and object key."""
        s3_url = s3_url.replace('s3://', '')
        parts = s3_url.split('/', 1)
        if len(parts) != 2:
            raise ValueError("Invalid S3 URL format")
        
        bucket_name = parts[0]
        object_key = parts[1]
        
        return bucket_name, object_key
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
