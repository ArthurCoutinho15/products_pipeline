from mongo_conn import Mongo
from etl import Etl

def mongo_conn():
    mongo = Mongo('mongodb://mongodb:27017/', 'Produtos', 'pipeline_produtos')
    #mongo = Mongo('mongodb://localhost:27017/', 'Produtos', 'pipeline_produtos')

    mongo.connect_mongo()
    print(mongo)
    
    return mongo.connect_collection()

def extract():      
    url = 'https://labdados.com/produtos'
    etl = Etl(url)
    data = etl.extract_data()
    
    return etl, data

def transform(etl, data):
    etl.transform_data(data)
    return data

def load_data_into_mongo(etl, data, collection):
    etl.load_data_into_mongo(data, collection)
    


if __name__ == '__main__':
    
    try:
        coll = mongo_conn()
        etl, data = extract()
        
        if not data.empty:
            data = transform(etl, data)
            if not data.empty:
                load_data_into_mongo(etl, data, coll)
                print("Pipeline concluído com sucesso!")
            else:
                print("Dados transformados vazios - carga cancelada")
        else:
            print("Dados extraídos vazios - pipeline interrompido")
            
    except Exception as e:
        print(f"Erro crítico: {str(e)}")
        raise
    
