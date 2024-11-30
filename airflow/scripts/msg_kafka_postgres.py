from airflow.hooks.postgres_hook import PostgresHook
import json
from kafka import KafkaProducer, KafkaAdminClient
from kafka.admin import NewTopic
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
    bootstrap_servers = 'kafka1:9092'
    topico = 'postgres'
    
    # Crear un cliente de administración para verificar y crear el tópico si no existe
    admin_client = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
    
    # Verificar si el tópico existe
    topics = admin_client.list_topics()
    if topico not in topics:
        # Crear el tópico si no existe
        topic = NewTopic(name=topico, num_partitions=1, replication_factor=1)
        admin_client.create_topics(new_topics=[topic], validate_only=False)
        print(f"Tópico '{topico}' creado.")
    else:
        print(f"Tópico '{topico}' ya existe.")
    
    # Crear el productor de Kafka
    productor_kafka = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
    # Obtener los registros nuevos desde XCom
    registros_nuevos = kwargs['ti'].xcom_pull(key='registros_nuevos', task_ids='obtener_registros_nuevos')
    
    # Enviar los registros al tópico 'postgres'
    for registro in registros_nuevos:
        productor_kafka.send(topico, registro)
    
    # Asegurarse de que todos los mensajes se envíen
    productor_kafka.flush()

    print(f"Se enviaron {len(registros_nuevos)} registros al tópico '{topico}'.")


def actualizar_conteos(**kwargs):
    conexion_postgres = PostgresHook(postgres_conn_id="conexion_postgres")
    consulta_total_registros = "SELECT COUNT(*) FROM medicamentos;"
    total_registros = conexion_postgres.get_first(consulta_total_registros)[0]
    
    insertar_conteo = """
        INSERT INTO conteos (base_datos, tabla, cantidad_registros, ultima_actualizacion)
        VALUES ('postgres', 'medicamentos', %s, NOW());
    """
    conexion_postgres.run(insertar_conteo, parameters=(total_registros,))