
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import requests




class pipeline():

    def __init__(self, nome_db, nome_collection):
        self.uri = "mongodb+srv://lucascavalheiro:12345@cluster-pipeline.nxgric5.mongodb.net/?appName=Cluster-pipeline"
        self.nome_db = nome_db
        self.nome_collection = nome_collection
        self.client = self.connection()
        self.db, self.collection = self.create_db()

    
    def connection(self):
        # Create a new client and connect to the server
        client = MongoClient(self.uri, server_api=ServerApi('1'))

        # Send a ping to confirm a successful connection
        try:
            client.admin.command('ping')
            print("Pinged your deployment. You successfully connected to MongoDB!")
        except Exception as e:
            print(e)
        
        return client



    def create_db(self):
        
        client = self.client

        db = client[self.nome_db]
        collection = db[self.nome_collection]

        return db, collection



    def extract_and_save(self, api_url):

        response = requests.get(api_url)
        data = response.json()

        self.collection.insert_many(data)
        



