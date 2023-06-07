import pandas as pd

class DataCleaning: 
    def clean_user_data(self, df):
        cleaned_data = df.copy()
        
        # Drop rows with NULL values
        cleaned_data.dropna(inplace=True)
        
        # Clean date errors and convert to datetime
        cleaned_data['date'] = pd.to_datetime(cleaned_data['date'], errors='coerce')
        
        # Clean incorrectly typed values
        cleaned_data['age'] = cleaned_data['age'].astype(int)
        
        # Remove rows with wrong information
        cleaned_data = cleaned_data[cleaned_data['age'] > 0]  # Example: Age should be positive
        
        return cleaned_data


