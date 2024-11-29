import pandas as pd
from sqlalchemy import create_engine
import json
import os

# Configuración de conexión a PostgreSQL
host = '192.168.1.65'  # Cambia si tu PostgreSQL está en otro host
port = '3307'  # Puerto por defecto de PostgreSQL
dbname = 'salazar'  # Nombre de la base de datos
user = 'admin'  # Tu usuario de PostgreSQL
password = 'root'  # Tu contraseña de PostgreSQL

# Ruta del archivo CSV
csv_file_path = '/opt/airflow/scripts/datos/extranjeros.csv'

# Crear conexión a PostgreSQL usando SQLAlchemy
engine = create_engine(f'mysql+pymysql://{user}:{password}@{host}:{port}/{dbname}')

# Leer el archivo CSV con pandas
df = pd.read_csv(csv_file_path)

# Subir el DataFrame a PostgreSQL (se crea la tabla automáticamente)
df.to_sql('extranjeros', engine, if_exists='replace', index=False)

# Eliminar el archivo CSV después de cargarlo
os.remove(csv_file_path)

print(f"Datos cargados exitosamente en la tabla 'extranjeros'.")