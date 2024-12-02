from pymongo import MongoClient
from kafka import KafkaProducer, KafkaAdminClient
from kafka.admin import NewTopic
from datetime import datetime
import json


def obtener_registros_nuevos_mongo(**kwargs):
    cliente_mongo = MongoClient("mongodb://192.168.1.65:27017")
    db = cliente_mongo["salazarPostgres"]
    coleccion = db["contratos"]
    
    metadatos = db["conteos"]
    ultimo_conteo = metadatos.find_one({"tabla": "contratos"})
    ultimo_conteo = ultimo_conteo["cantidad_registros"] if ultimo_conteo else 0
    
    registros_nuevos = list(coleccion.find().skip(ultimo_conteo))

    registros_nuevos = [str(registro['_id']) for registro in registros_nuevos]

    kwargs['ti'].xcom_push(key='registros_nuevos', value=registros_nuevos)
    return registros_nuevos

def enviar_a_kafka_mongo(**kwargs):
    bootstrap_servers = 'kafka1:9092'
    topico = 'mongo'

    admin_client = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
    topics = admin_client.list_topics()

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
    
    registros_nuevos = kwargs['ti'].xcom_pull(key='registros_nuevos', task_ids='obtener_registros_nuevos_mongo')

    for registro in registros_nuevos:
        productor_kafka.send(topico, registro)

    productor_kafka.flush()
    print(f"Se enviaron {len(registros_nuevos)} registros al tópico '{topico}'.")

def actualizar_conteos_mongo(**kwargs):
    cliente_mongo = MongoClient("mongodb://192.168.1.65:27017")
    db = cliente_mongo["salazarPostgres"]
    coleccion = db["contratos"]
    metadatos = db["conteos"]

    total_registros = coleccion.count_documents({})
    
    metadatos.insert_one({
        "base_datos": "mongo",
        "tabla": "contratos",
        "cantidad_registros": total_registros,
        "ultima_actualizacion": datetime.utcnow()
    })

    bootstrap_servers = 'kafka1:9092'
    topic = 'estadisticas'

    admin_client = KafkaAdminClient(bootstrap_servers=bootstrap_servers)

    topics = admin_client.list_topics()
    if topic not in topics:
        new_topic = NewTopic(name=topic, num_partitions=1, replication_factor=1)
        admin_client.create_topics(new_topics=[new_topic], validate_only=False)
        print(f"Tópico '{topic}' creado.")
    else:
        print(f"Tópico '{topic}' ya existe.")

    productor_kafka = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    mensaje = {
        "base_datos": "mongo",
        "tabla": "contratos",
        "cantidad_registros": total_registros,
        "ultima_actualizacion": datetime.utcnow().isoformat()
    }

    productor_kafka.send(topic, mensaje)
    productor_kafka.flush()

    print(f"Conteo de MongoDB enviado a Kafka: {mensaje}")