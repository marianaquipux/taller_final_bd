from pymongo import MongoClient
from kafka import KafkaProducer, KafkaAdminClient
from kafka.admin import NewTopic
from datetime import datetime
import json

def obtener_registros_nuevos_mongo(**kwargs):
    # Conexión a MongoDB
    cliente_mongo = MongoClient("mongodb://usuario:contraseña@host:puerto")
    db = cliente_mongo["nombre_base_datos"]
    coleccion = db["contratos"]  # Actualizado a "contratos"
    
    # Obtener el último conteo almacenado en una colección de metadatos
    metadatos = db["conteos"]
    ultimo_conteo = metadatos.find_one({"tabla": "contratos"})
    ultimo_conteo = ultimo_conteo["cantidad_registros"] if ultimo_conteo else 0
    
    # Obtener registros nuevos
    registros_nuevos = list(coleccion.find().skip(ultimo_conteo))
    
    # Enviar registros nuevos a XCom
    kwargs['ti'].xcom_push(key='registros_nuevos', value=registros_nuevos)
    return registros_nuevos


def enviar_a_kafka_mongo(**kwargs):
    # Configuración de Kafka
    bootstrap_servers = 'kafka1:9092'
    topico = 'mongo'

    admin_client = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
    topics = admin_client.list_topics()

    # Crear el tópico si no existe
    if topico not in topics:
        topic = NewTopic(name=topico, num_partitions=1, replication_factor=1)
        admin_client.create_topics(new_topics=[topic], validate_only=False)
        print(f"Tópico '{topico}' creado.")
    else:
        print(f"Tópico '{topico}' ya existe.")

    productor_kafka = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
    # Obtener registros nuevos de XCom
    registros_nuevos = kwargs['ti'].xcom_pull(key='registros_nuevos', task_ids='obtener_registros_nuevos_mongo')

    # Enviar registros a Kafka
    for registro in registros_nuevos:
        productor_kafka.send(topico, registro)

    productor_kafka.flush()
    print(f"Se enviaron {len(registros_nuevos)} registros al tópico '{topico}'.")


def actualizar_conteos_mongo(**kwargs):
    # Conexión a MongoDB
    cliente_mongo = MongoClient("mongodb://192.168.1.65:27017")
    db = cliente_mongo["salazarPostgres"]
    coleccion = db["contratos"]  # Actualizado a "contratos"
    metadatos = db["conteos"]

    # Obtener el total de registros
    total_registros = coleccion.count_documents({})
    
    # Actualizar conteo en la colección de metadatos
    metadatos.insert_one({
        "base_datos": "mongo",
        "tabla": "contratos",
        "cantidad_registros": total_registros,
        "ultima_actualizacion": datetime.utcnow()
    })
