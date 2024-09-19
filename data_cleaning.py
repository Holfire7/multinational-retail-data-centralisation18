import pandas as pd
import numpy as np
from datetime import datetime
import re



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

        # 2. Deduplicate data if necessary
        df.drop_duplicates(inplace=True)

        # 3. Reset the index after cleaning
        df.reset_index(drop=True, inplace=True)

        return df
    
    def clean_address(self, address: str) -> str:
        """Cleans the address field."""
        address = address.replace('\n', ' ')
        address = address.strip()
        return address

    def convert_product_weights(self, products_df: pd.DataFrame) -> pd.DataFrame:
        """Converts product weights to kilograms and cleans up the weight column."""
        def convert_weight(weight):
            weight = str(weight).strip().lower()
            if not weight:
                return None
            number = re.findall(r"[\d.]+", weight)
            if not number:
                return None
            number = float(number[0])
            if 'kg' in weight:
                return number
            if 'g' in weight or 'ml' in weight:
                return number / 1000
            if 'lb' in weight:
                return number * 0.453592
            if 'oz' in weight:
                return number * 0.0283495
            return None

        # Convert and clean the weight column
        if 'weight' in products_df.columns:
            products_df['weight'] = products_df['weight'].apply(convert_weight)
            products_df = products_df.dropna(subset=['weight'])  # Remove rows with invalid weights
        
        return products_df
    
    def clean_products_data(self, products_df: pd.DataFrame) -> pd.DataFrame:
        """Cleans the product data by handling missing values and erroneous entries."""

        products_df = products_df.copy()
        
        # Handle missing values
        products_df.fillna({
            'product_name': 'Unknown',
            'weight': 0
        }, inplace=True)
        
        
        # Handle erroneous weight entries
        if 'weight' in products_df.columns:
            products_df['weight'] = pd.to_numeric(products_df['weight'], errors='coerce')
            products_df = products_df[products_df['weight'] >= 0]  # Remove rows with negative weights
        
        #Deduplicate data if necessary
        products_df.drop_duplicates(inplace=True)
        
        # Reset the index after cleaning
        products_df.reset_index(drop=True, inplace=True)
        
        return products_df
    
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