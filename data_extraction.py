from sqlalchemy import create_engine, text
import pandas as pd
import tabula.io as tabula
import requests

class DataExtractor:            
    def read_rds_table(self, db_connector, table_name):
        engine = db_connector.init_db_engine()
        query = f'SELECT * FROM {table_name}'

        df = pd.read_sql_query(sql=text(query), con=engine.connect())
        # df = pd.read_sql_table(table_name, con = engine, index_col = 'index')
        return df
    
    def retrieve_pdf_data(self, link):
        pdf_wrapper = tabula.read_pdf(link, pages = 'all', multiple_tables = True)
        extracted_data = pd.concat(pdf_wrapper)   
        return extracted_data
    
    def __init__(self):
        self.headers = {'x-api-key': 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}
    
    def list_number_of_stores(self, endpoint, headers):
        response = requests.get(endpoint, headers=headers)
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
        for store_number in range(1, number_of_stores + 1):
            url = store_endpoint.format(store_number)
            response = requests.get(url, headers = headers)
            if response.status_code == 200:
                store_data = response.json()
                stores_data.append(store_data)
            else:
                print(f"Failed to retrieve data for store {store_number}. Status code: {response.status_code}")

        df = pd.DataFrame(stores_data)
        return df
        

