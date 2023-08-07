# Multinational Retail Data Centralisation Project

##### This project utilises a local PostgreSQL database where we have uploaded data from various sources, processed it and created a database scheme to run SQL queries.
##### We have used: Postgres, AWS (s3), boto3, rest-API, csv, Python (Pandas, Numpy, sqlalchemy, tabula, re, uuid, requests, datatime) 

## Utility Functions for the Project
##### 1. Database connection: This is done through a DatabaseConnector class in "database_utils.py" which initiates the database engine based on the credentials provided by the yaml files. 
##### 2. Data extraction: This is done through a "data_extraction.py" file where we have methods stored to upload the data into a pandas data frame from different sources. 
##### 3. Data cleaning: This is done through a "data_cleaning.py" where we clean the unneccessary data that was extracted that could hinder any future analysis or SQL queries.
##### 4. Data Pipeline: This is where all my data is pipelined through that contains all the methods, in order to upload data directly into the local database (local PostgreSQL in this case) 

## Data processing
##### We have a 6 data sources for our project:
##### 1. Remote Postgres DB (AWS Cloud) - "order_table" contains sales data of interest. Fields: "date_uuid", "user_uuid", "card_number", "store_code", "product_code", and "product_quantity". Clean and handle NaNs, convert "product_quantity" to integer.
##### 2. Remote Postgres DB (AWS Cloud) - "dim_users" table for user data. Use same upload techniques. Primary key: "user_uuid".
##### 3. Public link (AWS Cloud) - "dim_card_details" accessible as ".pdf" on S3 server. Read with "tabula" package. Primary key: card number. Convert to string and clean.
##### 4. AWS S3 bucket - "dim_product" table. Use "boto3" to download. Primary key: "product_code". Convert "product_price" to float, standardize "weight" to grams.
##### 5. Restful API - Access "dim_store_details" data using GET method. Convert ".json" response to pandas dataframe. Primary key: "store_code".
##### 6. "dim_date_times" data available through a link. Convert ".json" response to pandas dataframe. Primary key: "date_uuid".

## Explaining the process
##### The purpose of data cleaning is to mainly focus on the "primary key" field. So it will remove rows of tables if they are duplicates (ie: NaNs, missing values...). This is to ensure the "foreign key" in "orders_table" are no compromised or the database schema will not work.

## SQL Queries
##### Once we have uploaded the extracted and cleaned data, and primary and foreign keys are settled, we then proceed to head to my local database under pgAdmin4 where through PostgreSQL we proceed to fulfil the SQL query tasks set.
[sql_results](sql_queries.md)

