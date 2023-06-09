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
    
    def convert_product_weights(self, df):
        df['weight'] = df['weight'].str.replace(r'\D', '', regex=True).astype(float)
        df.loc[df['weight'] == 'ml', 'weight'] *= 1 # Convert ml to g using 1:1 ratio

        return df
    
    def clean_products_data(self,df):
        df = df.drop_duplicates()
        df = df.dropna()

        return df 
    
    def clean_orders_data(self, orders_data):

        columns_to_remove = ['first_name', 'last_name', '1']
        cleaned_orders_data = orders_data.drop(columns = columns_to_remove)

        return cleaned_orders_data
    
    def clean_date_data(self, date_data):
        cleaned_date_data = date_data.copy()

        cleaned_date_data.dropna(inplace = True)

        return cleaned_date_data
   
    
    
    
    


    

    





