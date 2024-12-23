import pandas as pd
from sqlalchemy import create_engine
import os


host = '192.168.1.65'
port = '5433'
dbname = 'salazarPostgres'
user = 'admin'
password = 'root'

csv_file_path = '/opt/airflow/scripts/datos/medicamentos.csv'

engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{dbname}')

df = pd.read_csv(csv_file_path)
print(df)

df.to_sql('medicamentos', engine, if_exists='replace', index=False)

os.remove(csv_file_path)

print(f"Datos cargados exitosamente en la tabla 'medicamentos'.")
