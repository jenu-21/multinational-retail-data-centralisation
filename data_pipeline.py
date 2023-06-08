from database_utils import DatabaseConnector
from data_extraction import DataExtractor
from data_cleaning import DataCleaning

# Create instances of necessary classes
db_connector = DatabaseConnector()
data_extractor = DataExtractor()
data_cleaner = DataCleaning()

number_of_stores_endpoint = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'
headers = {'x-api-key': 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}
number_of_stores = data_extractor.list_number_of_stores(number_of_stores_endpoint, headers)
print(f"Number of stores to extract: {number_of_stores}")

store_endpoint = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store_number}'
print(f"Number of stores: {number_of_stores}")

store_data = data_extractor.retrieve_stores_data(store_endpoint, number_of_stores)

cleaned_store_data = data_cleaner.clean_store_data(store_data)


# link = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf'
# pdf_data = data_extractor.retrieve_pdf_data(link)

# tables = db_connector.list_db_tables()

# pdf_data = data_extractor.retrieve_pdf_data(link)

# cleaned_data = data_cleaner.clean_card_data(pdf_data)

table_name = 'dim_store_details'
db_connector.upload_to_db(cleaned_store_data, table_name)


