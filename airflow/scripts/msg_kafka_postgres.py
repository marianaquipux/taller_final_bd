from airflow.hooks.postgres_hook import PostgresHook
from kafka import KafkaProducer
import json

def obtener_registros_nuevos(**kwargs):
    conexion_postgres = PostgresHook(postgres_conn_id="conexion_postgres")
    
    consulta_ultimo_conteo = """
        SELECT cantidad_registros 
        FROM conteos 
        WHERE tabla = 'medicamentos' 
        ORDER BY ultima_actualizacion DESC 
        LIMIT 1;
    """
    ultimo_conteo = conexion_postgres.get_first(consulta_ultimo_conteo)
    ultimo_conteo = ultimo_conteo[0] if ultimo_conteo else 0
    
    consulta_nuevos_registros = f"SELECT * FROM medicamentos OFFSET {ultimo_conteo};"
    registros_nuevos = conexion_postgres.get_records(consulta_nuevos_registros)
    
    kwargs['ti'].xcom_push(key='registros_nuevos', value=registros_nuevos)
    return registros_nuevos

def enviar_a_kafka(**kwargs):
    productor_kafka = KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    registros_nuevos = kwargs['ti'].xcom_pull(key='registros_nuevos', task_ids='obtener_registros_nuevos')
    topico = 'postgres'
    for registro in registros_nuevos:
        productor_kafka.send(topico, registro)
    productor_kafka.flush()

def actualizar_conteos(**kwargs):
    conexion_postgres = PostgresHook(postgres_conn_id="conexion_postgres")
    consulta_total_registros = "SELECT COUNT(*) FROM medicamentos;"
    total_registros = conexion_postgres.get_first(consulta_total_registros)[0]
    
    insertar_conteo = """
        INSERT INTO conteos (base_datos, tabla, cantidad_registros, ultima_actualizacion)
        VALUES ('postgres', 'medicamentos', %s, NOW());
    """
    conexion_postgres.run(insertar_conteo, parameters=(total_registros,))