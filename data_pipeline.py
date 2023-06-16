from database_utils import DatabaseConnector
from data_extraction import DataExtractor
from data_cleaning import DataCleaning

# Create instances of necessary classes
db_connector = DatabaseConnector()
data_extractor = DataExtractor()
data_cleaner = DataCleaning()

s3_address = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json'
date_data = data_extractor.extract_from_s3(s3_address)
cleaned_date_data = data_cleaner.clean_date_data(date_data)
table_name = 'dim_date_times'
db_connector.upload_to_db(cleaned_date_data, table_name)

s3_address = 's3://data-handling-public/products.csv'
products_data = data_extractor.extract_from_s3(s3_address)
cleaned_products_data = data_cleaner.convert_product_weights(products_data)
cleaned_products_data= data_cleaner.clean_products_data(cleaned_products_data)
table_name = 'dim_products'
db_connector.upload_to_db(cleaned_products_data, table_name)
tables = db_connector.list_db_tables()
print("Tables in the database:", tables)

orders_table_name = 'orders_table'
orders_data = data_extractor.read_rds_table(db_connector, orders_table_name)
cleaned_orders_data = data_cleaner.clean_orders_data(orders_data)
cleaned_table_name = 'orders_table'
db_connector.upload_to_db(cleaned_orders_data, cleaned_table_name)


number_of_stores_endpoint = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'
store_endpoint = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{}'
headers = {'x-api-key': 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}

number_of_stores = data_extractor.list_number_of_stores(number_of_stores_endpoint, headers)
if number_of_stores is None:    
   print("Unable to retreive the number of stores. Exiting...")
   exit()

print(f"Number of stores to extract: {number_of_stores}")

store_data = data_extractor.retrieve_stores_data(store_endpoint, number_of_stores, headers)
if store_data is None:
    print("Unable to retrieve store data. Exiting...")
    exit()

print(f"Number of stores extracted: {len(store_data)}")

cleaned_store_data = data_cleaner.clean_store_data(store_data)
print("Cleaned store data:")
print(cleaned_store_data)
table_name = 'dim_store_details'
db_connector.upload_to_db(cleaned_store_data, table_name)

link = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf'
pdf_data = data_extractor.retrieve_pdf_data(link)
tables = db_connector.list_db_tables()
pdf_data = data_extractor.retrieve_pdf_data(link)
cleaned_data = data_cleaner.clean_card_data(pdf_data)
table_name = 'dim_card_details'
db_connector.upload_to_db(cleaned_data , table_name)

table_name = 'dim_users'
users_data = data_extractor.read_rds_table(db_connector, table_name)
cleaned_users_data = data_cleaner.clean_user_data(users_data)
db_connector.upload_to_db(cleaned_users_data, table_name)



