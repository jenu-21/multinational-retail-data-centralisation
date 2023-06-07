import pandas as pd

class DataExtractor:            
    def read_rds_table(self, db_connector, table_name):
        query = f"SELECT * FROM {table_name}"
        df = pd.read_sql_query(query, db_connector.engine)
        return df

