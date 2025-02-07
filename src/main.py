from mongo_conn import Mongo


mongo = Mongo('mongodb://localhost:27017/', 'Produtos', 'pipeline_produtos')
mongo.connect_mongo()
print(mongo)