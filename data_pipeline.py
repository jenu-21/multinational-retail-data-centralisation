from database_utils import DatabaseConnector
from data_extraction import DataExtractor
from data_cleaning import DataCleaning

# Create instances of necessary classes
db_connector = DatabaseConnector()
data_extractor = DataExtractor()
data_cleaner = DataCleaning()

# Extract data from RDS database
tables = db_connector.list_db_tables('db_creds.yaml')
user_table = 'user_data'  # Assuming the user data is in a table named 'user_data'
if user_table in tables:
    user_data = data_extractor.read_rds_table(db_connector, user_table)

    # Cleaning the user data
    cleaned_user_data = data_cleaner.clean_user_data(user_data)

    # Upload cleaned data to the database
    db_connector.upload_to_db(cleaned_user_data, 'dim_users')
else:
    print(f"Table '{user_table}' not found in the database.")
