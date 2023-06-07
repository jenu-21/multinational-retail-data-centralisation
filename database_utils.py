import yaml
import psycopg2
from sqlalchemy import create_engine, inspect

class DatabaseConnector:
    def read_db_creds(self,  config_file ='db_creds.yaml'):
            with open(config_file, 'r') as f:
                config = yaml.safe_load(f)
            return config
       
    def init_db_engine(self):
        credentials = self.read_db_creds()
        db_url = f"postgresql+psycopg2://{credentials['RDS_USER']}:{credentials['RDS_PASSWORD']}@{credentials['RDS_HOST']}:{credentials['RDS_PORT']}/{credentials['RDS_DATABASE']}"
        engine = create_engine(db_url)
        return engine

    def list_db_tables(self):
        engine = self.init_db_engine()
        inspector = inspect(engine)
        tables = inspector.get_table_names()
        return tables

    def upload_to_db(self, df, table_name):
        try:
            with self.engine.begin() as connection:
                df.to_sql(table_name, connection, if_exists = 'replace', index=False)
            print(f"Data uploaded to the '{table_name}' table successfully!")       
        except Exception as e:
            print(f"Failed to upload data to the '{table_name}' table. Error {e}")




    