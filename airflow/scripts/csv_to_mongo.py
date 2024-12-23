import pandas as pd
from pymongo import MongoClient
import os

# Parámetros de conexión MongoDB
host = '192.168.1.65'
port = 27017
dbname = 'salazarPostgres'  
csv_file_path = '/opt/airflow/scripts/datos/contratos.csv'  

def cargar_csv_a_mongo(csv_file_path, host, port, dbname):
    mongo_db = MongoClient(host=host, port=port)
    db = mongo_db[dbname]
    
    df = pd.read_csv(csv_file_path)
    data_dict = df.to_dict(orient='records')
    
    # Seleccionar la colección (si no existe, se crea automáticamente)
    collection = db['contratos']
    
    # Insertar los datos en la colección de MongoDB
    collection.insert_many(data_dict)


# Llamada a la función para cargar los datos
cargar_csv_a_mongo(csv_file_path, host, port, dbname)

os.remove(csv_file_path)

print(f"Datos cargados exitosamente en la tabla 'medicamentos'.")