import requests
import pandas as pd
import pymongo
class Etl():
    def __init__(self, url):
        self.url = url
    
    def extract_data(self):
        response = requests.get(self.url).json()
        data = pd.DataFrame(response)
        
        return data
    
    def transform_data(self, data):
        data = data.rename(columns={'lat':'Latitude', 'lon':'Longitude'})
        data['Frete'] = data['Frete'].round(2)
        
        return data
        
    def load_data_into_mongo(self, data, collection):
        
        result = collection.insert_many(data.to_dict(orient='records'))
        n_rows_inserted = len(result.inserted_ids)
        return n_rows_inserted
    
    
        
    
    