from sqlalchemy import create_engine, text
import sqlalchemy
import pandas as pd
import tabula.io as tabula
import requests
import boto3

class DataExtractor:   

    def read_rds_table(self, db_connector, table_name):
        engine = db_connector.init_db_engine()
        query = sqlalchemy.text(f'SELECT * FROM {table_name}')

        with engine.connect() as connection:
            result = connection.execute(query)
            column_names = result.keys()
            rows = result.fetchall()
            df = pd.DataFrame(rows, columns=column_names)
            # df = pd.read_sql_query(sql=text(query), con=engine.connect())
            # df = pd.read_sql_table(table_name, con = engine, index_col = 'index')
            # df.to_csv('dirty_users.csv')
        
        return df

        
        
        # df = pd.read_sql_query(sql=text(query), con=engine.connect())
        # df = pd.read_sql_table(table_name, con = engine, index_col = 'index')
        # return df
    
    def retrieve_pdf_data(self, link):
        pdf_wrapper = tabula.read_pdf(link, pages = 'all', multiple_tables = True)
        extracted_data = pd.concat(pdf_wrapper)   
        return extracted_data
    
    def __init__(self):
        self.headers = {'x-api-key': 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}
    
    def list_number_of_stores(self, number_of_stores_endpoint, headers):
        response = requests.get(number_of_stores_endpoint, headers=headers)
        print(f"Response: {response.json()}")
        if response.status_code == 200:
            number_of_stores = response.json().get('number_stores')
            return number_of_stores
        else:
            print(f"Failed to retrieve store data. Status code: {response.status_code}")
            return None

    def retrieve_stores_data(self, store_endpoint, number_of_stores, headers):
        if number_of_stores is None:
            print("Number of stores is not available. Cannot retrieve store data")
            return None
        
        stores_data = []
        for store_number in range(0, number_of_stores + 1):
            url = store_endpoint.format(store_number)
            response = requests.get(url, headers = headers)
            if response.status_code == 200:
                store_data = response.json()
                stores_data.append(store_data)
            else:
                print(f"Failed to retrieve data for store {store_number}. Status code: {response.status_code}")

        df = pd.DataFrame(stores_data)
        return df

    
    
    def extract_from_s3(self, s3_address):
        bucket_name, key = self.parse_s3_address(s3_address)

        session = boto3.Session()
        s3_client = session.client('s3')

        local_file_path = 'products.csv'
        print(f"Local file path: {local_file_path}")

        s3_client.download_file(bucket_name, key, local_file_path)
        print(f"File downloaded from S3")

        df = pd.read_csv('products.csv')
        
        return df
    
    def parse_s3_address(self, s3_address):
        
        s3_address = s3_address.replace('s3://', '')
        parts = s3_address.split('/', 1)
        bucket_name = parts[0]
        key = parts[1] if len(parts) > 1 else ''

        return bucket_name, key

    def extract_from_s3(self, s3_address):
        try:
            response = requests.get(s3_address)
            response.raise_for_status()
            data = response.json()
            df = pd.DataFrame(data)
            return df
        except Exception as e:
            print(f"Failed to extract data from S3. Error: {e}")
            return None  
     
    def parse_s3_address(self, s3_address):
        s3_address = s3_address.replace('https://', '')
        parts = s3_address.split('/', 1)
        bucket_name = parts[0]
        key = parts[1] if len(parts) > 1 else ''

        return bucket_name, key
            


       


