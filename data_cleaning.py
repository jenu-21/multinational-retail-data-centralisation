import pandas as pd
import numpy as np
import uuid
from sqlalchemy import create_engine, text
from database_utils import DatabaseConnector 
import datetime
import re

class DataCleaning: 
    def __init__(self):
        self.db_connector = DatabaseConnector()
    
    def isDigits(self,num):
        return str(num) if str(num).isdigit() else np.nan


    def clean_user_data(self, df):
        cleaned_data = df.copy()
        
        # Drop rows with NULL values
        cleaned_data.dropna(inplace=True)

        # Change data types of specific columns
        
        cleaned_data['join_date'] =  pd.to_datetime(df['join_date'], format='%Y-%m-%d',errors='coerce')
        cleaned_data['country_code'] = cleaned_data['country_code'].replace({'GGB':'GB'})
        cleaned_data=cleaned_data[cleaned_data['country_code'].isin(['GB','US','DE'])]
        cleaned_data['phone_number'] = cleaned_data['phone_number'].replace({'239.711.3836': '2397113836'})

        # unique_data = cleaned_data['date_of_birth'].unique()
        # print(unique_data)
        cleaned_data.to_csv('clean_users.csv')

        return cleaned_data
    
    def clean_card_data(self, df):
        cleaned_data = df.copy()

        cleaned_data['card_number'] = cleaned_data['card_number'].apply(str)
        cleaned_data['card_number'] = cleaned_data['card_number'].str.replace('?','')

        # Remove rows with invalid card_number length
        cleaned_data = cleaned_data[cleaned_data['card_number'].apply(lambda x: len(x) == 16)]

        cleaned_data['date_payment_confirmed'] = pd.to_datetime(cleaned_data['date_payment_confirmed'], format='%Y-%m-%d', errors='coerce')
        cleaned_data['expiry_date'] = cleaned_data['expiry_date'].str.extract(r'(\d{2}/\d{2})')
        cleaned_data.dropna(subset=['expiry_date'], inplace=True)
        cleaned_data.dropna(how='all', inplace=True)

        # unique_data = cleaned_data['card_provider'].unique()
        # print(unique_data)
        cleaned_data.to_csv('clean_card.csv') 
        
        return cleaned_data
    
    def clean_store_data(self, df):
        cleaned_data = df.copy()

        cleaned_data.drop(columns = 'lat', inplace=True)
        cleaned_data['opening_date'] = pd.to_datetime(cleaned_data['opening_date'], format='%Y-%m-%d', errors='coerce')
        cleaned_data['staff_numbers'] = pd.to_numeric(cleaned_data['staff_numbers'], errors='coerce')

        unique_values_to_clean = ['NULL', '6FWDZHD7PW', '1ZVU03X2P6', '13KJZ890JH', '9IBH8Y4Z0S', 'NRQKZWJ9OZ', 'BIP8K8JJW2', 'ZCXWWKF45G', 'QP74AHEQT0', '1CJ5OAU4BR', 'YELVM536YT', 'QMAVR5H3LD', 'UBCIFQLSNY', 'Q1TJY8H1ZH', '2XE1OWOC23', '1T6B406CI8', 'QIUU9SVP51', 'SKBXAXF5G5', '7AHXLXIUEF', 'O0QJIRC943', '3ZR3F89D97', 'FP8DLXQVGH', 'LU3E036ZD9', 'RC99UKMZB2', '2YBZ1440V6', 'OXVE5QR07O', '6LVWPU1G64', 'Y8J0Z2W8O9', '2429OB3LMM', '0OLAK2I6NS', '50IB01SFAZ', 'L13EQEQODP', 'HMHIFNLOBN', '5586JCLARW', 'X349GIDWKU', 'O7NF1FZ74Y', 'VKA5I8H32X', 'RX9TCP2RGB', 'ISEE8A57FE',  '74BY7HSB6P',  'A3PMVM800J', '0RSNUU3DF5',  'J3BPB68Z1J', 'F3AO8V2LHU', 'GFJQ2AAEQ8', 'ZBGB54ID4H', 'SKO4NMRNNF',  'LACCWDI0SB', 'CQMHKI78BX', 'T0R2CQBDUS', 'GT1FO6YGD4', 'GMMB02LA9V', 'B4KVQB3P5Y', 'AJHOMDOHZ4', 'OH20I92LX3', 'SLQBD982C0', 'XTUAV57DP4', 'ID819KG3X5', 'A3O5CBWAMD', 'RY6K0AUE7F', 'TUOKF5HAAQ', 'FRTGHAA34B', '13PIY8GD1H',  'X0FE7E2EOG', 'AE7EEW4HSS', 'OYVW925ZL8', 'XQ953VS0FG', 'K0ODETRLS3', 'K8CXLZDP07', 'UXMWDMX1LC', '3VHFDNP8ET',	'9D4LK7X4LZ', 'D23PCWSM6S', '36IIMAQD58', 'NN04B3F6UQ',	'JZP8MIJTPZ', 'B3EH2ZGQAV', '1WZB1TE1HL',]
        cleaned_data.replace(unique_values_to_clean, pd.NA, inplace=True)

        cleaned_data['continent'] = cleaned_data['continent'].str.strip()
        cleaned_data['continent'] = cleaned_data['continent'].replace({'eeEurope': 'Europe'})
        cleaned_data['continent'] = cleaned_data['continent'].replace({'eeAmerica': 'America'})
        
        cleaned_data.dropna(how = 'any', inplace=True)
        # cleaned_data.to_csv('dirty_store.csv')

        return cleaned_data
    
    def clean_products_data(self,df):
        df = df.drop_duplicates()
        df = df.dropna()

        # unique_price= df['product_price'].unique()
        # print(unique_price)

        unique_values_to_clean = ['VLPCU81M30', 'XCD69KUI0K', 'S1YB74MLMJ', 'OO7KH8P79I', 'CCAVRB79VV', '7QB0Z9EW1G', 'T3QRRH7SRP', 'SDAV678FVD', 'LB3D71C025', 'ODPMASE7V7', 'WVPMHZP59U', 'BHPF2JTNKQ', 'PEPWA0NCVH', 'VIBLHHVPMN', 'H5N71TV8AY', 'OPSD21HN67', '9SX4G65YUX', 'N9D2BZQX63', 'C3NCA2CL35', 'E8EOGWOY8S', '09KREHTMWL', 'CP8XYQVGGU', 'BPSADIOQOK', 'BSDTR67VD90']
        df.replace(unique_values_to_clean, pd.NA, inplace=True)
        df.dropna(how = 'all', inplace=True)

        df.to_csv('dirty_products.csv')

        return df 
    
    def convert_product_weights(self, df):
        df['weight'] = df['weight'].str.replace(r'\D', '', regex=True).astype(float)
        df.loc[df['weight'] == 'ml', 'weight'] *= 1 # Convert ml to g using 1:1 ratio

        return df
    
    
    def clean_orders_data(self, orders_data):
        cleaned_orders_data = orders_data.copy()
       
        cleaned_orders_data.drop(columns = '1', inplace=True)
        cleaned_orders_data.drop(columns = 'first_name', inplace=True)
        cleaned_orders_data.drop(columns = 'last_name', inplace=True)
        cleaned_orders_data.drop(columns = 'level_0', inplace=True)
        cleaned_orders_data['card_number'] = cleaned_orders_data['card_number'].apply(self.isDigits)

        cleaned_orders_data.dropna(how = 'all', inplace=True)
        # columns_to_remove = ['first_name', 'last_name', '1', 'level_0']
        cleaned_orders_data.to_csv('clean_orders_data.csv')

        return cleaned_orders_data
    
    def clean_date_data(self, date_data):
        cleaned_date_data = date_data.copy()

        cleaned_date_data['month'] = pd.to_numeric(cleaned_date_data['month'], errors='coerce', downcast="integer")
        cleaned_date_data['day'] = pd.to_numeric(cleaned_date_data['day'], errors='coerce', downcast="integer")
        cleaned_date_data['year'] = pd.to_numeric(cleaned_date_data['year'], errors='coerce', downcast="integer")
        cleaned_date_data['timestamp'] = pd.to_datetime(cleaned_date_data['timestamp'], format='%H:%M:%S', errors='coerce')
        cleaned_date_data['timestamp'] = cleaned_date_data['timestamp'].dt.time
        valid_time_periods = ['Evening', 'Midday', 'Morning', 'Late_Hours']
        cleaned_date_data = cleaned_date_data[cleaned_date_data['time_period'].isin(valid_time_periods)]

        pattern = r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
        cleaned_date_data = cleaned_date_data[cleaned_date_data['date_uuid'].str.match(pattern, na=False)]



        # unique_values_to_clean = ['DXBU6GX1VC','OEOXBP8X6D','NULL', '1Z18F4RM05','GT3JKF575H', 'CM5MTJKXMH', '5OQGE7K2AV', '1JCRGU3GIE', 'SQX52VSNMM', 'ALOGCWS9Y3', '7DNU2UWFP7', 'EOHYT5T70F', '5MUU1NKRED', '7RR8SRXQAW', 'SSF9ANE440', '1PZDMCME1C', 'KQVJ34AINL', 'QA65EOIBX4', 'YRYN6Y8SPJ', 'JMW951JPZC', 'DZC37NLW4F', 'SYID3PBQLP', 'IXNB2XXEKB', 'MZIS9E7IXD', 'SAAZHF87TI', '1YMRDJNU2T', 'FTKRTQHFZE', '7EPFWYOELT', '3A21WYQSY7', '75E4ECDVH6', '9GN4VIO5A8', '14NRQ80L5E', 'K4702YOYPT', '66ULRXEWSU', 'JUVMW8TKUC', 'NF46JOZMTA', '33F45BZPSP', 'ZS0PDDW72O', 'XXHOTW6WA7', 'LZLLPZ0ZUA', 'YULO5U0ZAM', 'SAT4V9O2DL', '3ZZ5UCZR5D' 'DGQAH7M1HQ' '4FHLELF101', '22JSMNGJCU', 'EB8VJHYZLE', '2VZEREEIKB', 'K9ZN06ZS1X', '9P3C0WBWTU', 'W6FT760O2B', 'DOIR43VTCM', 'FA8KD82QH3', '03T414PVFI', 'FNPZFYI489', '67RMH5U2R6', 'J9VQLERJQO', 'ZRH2YT3FR8', 'GYSATSCN88','TTH9JE93YZ' 'CZ35WI1011' 'PYXMXY268K' 'XAOROIDDK6' 'DEQME0YBTK', 'DK93ZX02KL', '8XA0GSY2Z6', 'Y1D3T5NOZ8', '1OFIGIX6Q9', '41OFB3C3PD', '814572Q64C', 'WRBNFJ46LJ', 'PKTU8OYDWF', 'G3Z42NGBRE', 'XZJTZGW546', '83QG97URRC', '8GZBM34AGM', 'W5A22P7J9N', 'K179M0CI4M', 'WRTW643A1S','G3DEZY8UW6' '0M8BGI0CI3' 'O17F6WE1TD' '9DKC6PW41E' 'I5367BRUVN'
        #                           'EB2N507OZ0', 'KO7BGRPOKH', 'RA8D4CIQOV', 'FXC3K5LZZX', 'FIEOPTN7WZ'
        #                           'AQLUVY7DA2', '9QDY0WMH6K', '5RZL03AWR6', 'QF6S8TDTEA', 'L1N4X0SVZA'
        #                           'OVDJZCARJA', 'UDHIYJS2GP', 'LND1WX0Y6Z', '3GJWN253MM', 'M9ZV3N8G95', '3ZZ5UCZR5D',
        #                           'DGQAH7M1HQ', '4FHLELF101', 'G3DEZY8UW6', '0M8BGI0CI3', 'O17F6WE1TD', '9DKC6PW41E', 'I5367BRUVN',
        #                           'EB2N507OZ0', 'FIEOPTN7WZ', 'AQLUVY7DA2', 'L1N4X0SVZA', 'OVDJZCARJA', 'TTH9JE93YZ', 'CZ35WI1011', 'PYXMXY268K', 'XAOROIDDK6', 'DEQME0YBTK', 'J8CSDZCCRZ', 'LZLLPZ0ZUA', 'G3DEZY8UW6',	'TTH9JE93YZ','3TL8P43R9J']
        # cleaned_date_data.replace(unique_values_to_clean, pd.NA, inplace=True)
        cleaned_date_data.dropna(how = 'all', inplace=True)
        

        # unique_data = cleaned_date_data['date_uuid'].unique()
        # print(unique_data)
        # cleaned_date_data.to_csv('dirty_date.csv')

        return cleaned_date_data
   
    
    
    
    


    

    





