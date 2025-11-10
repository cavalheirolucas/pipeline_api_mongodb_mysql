from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import pandas as pd
import requests
import mysql.connector





class pipeline():

    def __init__(self, nome_db, nome_collection):
        self.uri = "mongodb+srv://lucascavalheiro:12345@cluster-pipeline.nxgric5.mongodb.net/?appName=Cluster-pipeline"
        self.nome_db = nome_db
        self.nome_collection = nome_collection
        self.client = self.connection()
        self.db, self.collection = self.create_db()
        self.host = "localhost"
        self.user = "licascavalheiro"
        self.password = "12345"

    
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
        collection.close()
        


    def save(self, path, db_name, tbl_name):

        cnx = mysql.connector.connect(
            host=self.host,
            user=self.user,
            password=self.password
        )
        cursor = cnx.cursor()

        # cria banco e seleciona
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {db_name};")
        cursor.execute(f"USE {db_name};")

        df_livros = pd.read_csv(path)

        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {tbl_name}(
                id VARCHAR(100) PRIMARY KEY,
                Produto VARCHAR(100),
                Categoria_Produto VARCHAR(100),
                Preco FLOAT(10,2),
                Frete FLOAT(10,2),
                Data_Compra DATE,
                Vendedor VARCHAR(100),
                Local_Compra VARCHAR(100),
                Avaliacao_Compra INT,
                Tipo_Pagamento VARCHAR(100),
                Qntd_Parcelas INT,
                Latitude FLOAT(10,2),
                Longitude FLOAT(10,2)
            );
        """)

        lista_dados = [tuple(row) for i, row in df_livros.iterrows()]

        sql = f"""INSERT INTO {tbl_name}
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"""

        cursor.executemany(sql, lista_dados)

        cnx.commit()
        cnx.close()
