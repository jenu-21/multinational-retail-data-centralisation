import pandas as pd

class DataCleaning: 
    def clean_user_data(self, df):
        cleaned_data = df.copy()
        
        # Drop rows with NULL values
        cleaned_data.dropna(inplace=True)
         
        return cleaned_data
    
    def clean_card_data(self, df):
        cleaned_data = df.copy()

        card_columns = ['card_number', 'expiry_date', 'card_provider', 'date_payment_confirmed']
        cleaned_data.dropna(subset=card_columns, inplace=True)

        cleaned_data['card_number'] = cleaned_data['card_number'].str.replace('\s+', '', regex=True)
        cleaned_data['expiry_date'] = cleaned_data['expiry_date'].str.replace('\s+', '', regex=True)
        cleaned_data['card_provider'] = cleaned_data['card_provider'].str.strip()
        cleaned_data['date_payment_confirmed'] = pd.to_datetime(cleaned_data['date_payment_confirmed'], errors='coerce')
        
        return cleaned_data
    
    def clean_store_data(self, df):
        cleaned_data = df.copy()
        cleaned_data.drop_duplicates(inplace=True)
        
        return cleaned_data
    

    





