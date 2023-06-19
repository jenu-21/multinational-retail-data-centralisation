# Multinational Retail Data Centralisation Project

##### This project utilises a local PostgreSQL database where we have uploaded data from various sources, processed it and created a database scheme to run SQL queries.
##### We have used: Postgres, AWS (s3), boto3, rest-API, csv, Python (Pandas, Numpy, sqlalchemy, tabula, re, uuid, requests, datatime) 

## Utility Functions for the Project
##### 1. Database connection: This is done through a DatabaseConnector class in "database_utils.py" which initiates the database engine based on the credentials provided by the yaml files. 
##### 2. Data extraction: This is done through a "data_extraction.py" file where we have methods stored to upload the data into a pandas data frame from different sources. 
##### 3. Data cleaning: This is done through a "data_cleaning.py" where we clean the unneccessary data that was extracted that could hinder any future analysis or SQL queries.
##### 4. Data Pipeline: This is where all my data is pipelined through that contains all the methods, in order to upload data directly into the local database (local PostgreSQL in this case) 

##
