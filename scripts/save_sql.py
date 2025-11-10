import mysql.connector
import pandas as pd



class save_data():

    def __init__(self):
        self.host = "localhost"
        self.user = "licascavalheiro"
        self.password = "12345"

    
    def connection(self,path,db_name,tbl_name):

        cnx = mysql.connector.connect(
        host = self.host,
        user = self.user,
        password = self.password)

        cursor = cnx.cursor()
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {db_name};")

       
        df_livros = pd.read_csv(path)


        cursor.execute(f"""
    CREATE TABLE IF NOT EXISTS {db_name}.{tbl_name}(
               id VARCHAR(100),
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
               Longitude FLOAT(10,2),
               
               PRIMARY KEY (id));
""")


        lista_dados = [tuple(row) for i, row in df_livros.iterrows()]

        sql = f"INSERT INTO dbprodutos.{tbl_name} VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"

        cursor.executemany(sql, lista_dados)
        cnx.commit()

        cnx.close()