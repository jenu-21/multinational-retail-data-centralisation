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
        engine.connect()
        return engine
            
    def connect(self):
        try:
            self.conn = self.engine.connect()
            print("Connected to the database successfully!")
        except Exception as e:
            print(f"Failed to connect to the database. Error: {e}")

    def list_db_tables(self):
        engine = self.init_db_engine()
        inspector = inspect(engine)
        tables = inspector.get_table_names()
        return tables

    def upload_to_db(self, df, table_name):
        try:
            credentials = self.read_db_creds('local_creds.yaml')
            db_url = f"postgresql+psycopg2://{credentials['RDS_USER']}:{credentials['RDS_PASSWORD']}@{credentials['RDS_HOST']}:{credentials['RDS_PORT']}/{credentials['RDS_DATABASE']}"
            engine = create_engine(db_url)
           
            with engine.connect() as connection:
                df.to_sql(name = table_name, con = engine, if_exists = 'replace', index=False)
                             
        except Exception as e:
            print(f"Failed to upload data to the '{table_name}' table. Error {e}")

    