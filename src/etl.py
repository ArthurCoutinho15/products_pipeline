import requests
import pandas as pd
import pymongo
class Etl():
    def __init__(self, url):
        self.url = url
    

    def extract_data(self):
        try:
            response = requests.get(self.url, timeout=10)
            response.raise_for_status()  # Lança erro para códigos 4xx/5xx
            return pd.DataFrame(response.json())
        except requests.exceptions.RequestException as e:
            print(f"Erro na requisição: {str(e)}")
            return pd.DataFrame()  # Retorna DataFrame vazio
        except ValueError as e:
            print(f"Erro ao decodificar JSON: {str(e)}")
            return pd.DataFrame()
    
    def transform_data(self, data):
        data = data.rename(columns={'lat':'Latitude', 'lon':'Longitude'})
        data['Frete'] = data['Frete'].round(2)
        
        return data
        
    def load_data_into_mongo(self, data, collection):
        try:
            if data.empty:
                print("Aviso: DataFrame vazio. Nenhum dado inserido.")
                return 0
            
            records = data.to_dict(orient='records')
            print(f"Primeiro registro exemplo: {records[0]}")  # Log de exemplo
            
            result = collection.insert_many(records)
            print(f"Documentos inseridos: {len(result.inserted_ids)}")
            return len(result.inserted_ids)
            
        except pymongo.errors.PyMongoError as e:
            print(f"Erro MongoDB: {str(e)}")
            return 0
        except Exception as e:
            print(f"Erro inesperado: {str(e)}")
            return 0
    
    
        
    
    