from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import pandas as pd



class transform_data():

    def __init__(self, nome_db, nome_collection):
        self.uri = "mongodb+srv://lucascavalheiro:12345@cluster-pipeline.nxgric5.mongodb.net/?appName=Cluster-pipeline"
        self.client = self.connection()
        self.nome_db = nome_db
        self.nome_collection = nome_collection

    
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
    
    def transform_and_save(self,path):

        client = self.client

        db = client[self.nome_db]
        collection = db[self.nome_collection]

        collection.update_many({},{"$rename":{'lat':'Latitude','lon':'Longitude'}})

        query = {'Categoria do Produto':'livros'}

        lista_produto = []

        for i in collection.find(query):
            lista_produto.append(i)


        df = pd.DataFrame(lista_produto)

        df['Data da Compra']  = pd.to_datetime(df['Data da Compra'],format="%d/%m/%Y")
        df['Data da Compra'] = df['Data da Compra'].dt.strftime("%Y-%m-%d")
        df.to_csv(path, index=False)