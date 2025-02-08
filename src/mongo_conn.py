from pymongo import MongoClient

class Mongo():
    def __init__(self, uri, db_name, coll_name):
        self._uri = uri
        self._db_name = db_name
        self._coll_name = coll_name
        self._client = None
    
    def __str__(self):
        return f'Conexão Mongo: Uri={self._uri} | Database = {self._db_name} | Collection = {self._coll_name}'
    
    def connect_mongo(self):
        self._client = MongoClient(self._uri)
        try:
            self._client.admin.command('ping')
            print('Pinged your deployment. You sucessfully connected to MongoDB!')
        except Exception as e:
            print(e)
        

    def connect_db(self):
        db = self._client[self._db_name]
        return db
    
    def connect_collection(self):
        db = self.connect_db()
        return db[self._coll_name]