import pandas as pd
from sqlalchemy import create_engine, inspect

class DataExtractor:            
    def read_rds_table(self, db_connector, table_name):
        query = f"SELECT * FROM {table_name}"
        df = pd.read_sql_query(query, db_connector.engine)
        return df

