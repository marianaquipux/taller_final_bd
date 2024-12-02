from airflow.hooks.mysql_hook import MySqlHook 
import json
from kafka import KafkaProducer, KafkaAdminClient
from kafka.admin import NewTopic
from datetime import datetime


def verificar_y_crear_tabla_conteos_mysql():
    """
    Verifica si la tabla 'conteos' existe en MySQL, y si no, la crea.
    """
    conexion_mysql = MySqlHook(mysql_conn_id="conexion_mysql")
    consulta_crear_tabla_mysql = """
        CREATE TABLE IF NOT EXISTS conteos (
            id INT AUTO_INCREMENT PRIMARY KEY,
            base_datos VARCHAR(255) NOT NULL,
            tabla VARCHAR(255) NOT NULL,
            cantidad_registros INT NOT NULL,
            ultima_actualizacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
        );
    """
    conexion_mysql.run(consulta_crear_tabla_mysql)
    print("Tabla 'conteos' verificada o creada exitosamente en MySQL.")

def obtener_registros_nuevos_mysql(**kwargs):
    """
    Obtiene los registros nuevos desde la tabla 'extranjeros' en MySQL, basándose en el último conteo almacenado.
    """

    verificar_y_crear_tabla_conteos_mysql()

    conexion_mysql = MySqlHook(mysql_conn_id="conexion_mysql")

    consulta_ultimo_conteo_mysql = """
        SELECT cantidad_registros 
        FROM conteos 
        WHERE tabla = 'extranjeros' 
        ORDER BY ultima_actualizacion DESC 
        LIMIT 1;
    """
    ultimo_conteo = conexion_mysql.get_first(consulta_ultimo_conteo_mysql)
    ultimo_conteo = ultimo_conteo[0] if ultimo_conteo else 0

    consulta_nuevos_registros_mysql = f"SELECT * FROM extranjeros LIMIT {ultimo_conteo}, 1000;"  # Ajustado para obtener registros a partir del último conteo.
    registros_nuevos_mysql = conexion_mysql.get_records(consulta_nuevos_registros_mysql)
    
    kwargs['ti'].xcom_push(key='registros_nuevos_mysql', value=registros_nuevos_mysql)
    
    return registros_nuevos_mysql

def enviar_a_kafka_mysql(**kwargs):
    """
    Envía los registros nuevos a un tópico de Kafka.
    """
    bootstrap_servers = 'kafka1:9092'
    topico = 'mysql'

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

    registros_nuevos = kwargs['ti'].xcom_pull(key='registros_nuevos_mysql', task_ids='obtener_registros_nuevos_mysql')

    for registro in registros_nuevos:
        productor_kafka.send(topico, registro)

    productor_kafka.flush()
    print(f"Se enviaron {len(registros_nuevos)} registros al tópico '{topico}'.")


def actualizar_conteos_mysql(**kwargs):

    verificar_y_crear_tabla_conteos_mysql()

    conexion_mysql = MySqlHook(mysql_conn_id="conexion_mysql")

    consulta_total_registros_mysql = "SELECT COUNT(*) FROM extranjeros;"
    total_registros_mysql = conexion_mysql.get_first(consulta_total_registros_mysql)[0]

    insertar_conteo_mysql = """
        INSERT INTO conteos (base_datos, tabla, cantidad_registros, ultima_actualizacion)
        VALUES ('mysql', 'extranjeros', %s, NOW());
    """
    conexion_mysql.run(insertar_conteo_mysql, parameters=(total_registros_mysql,))

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
        "base_datos": "mysql",
        "tabla": "extranjeros",
        "cantidad_registros": total_registros_mysql,
        "ultima_actualizacion": datetime.utcnow().isoformat()
    }

    productor_kafka.send(topic, mensaje)
    productor_kafka.flush()

    print(f"Conteo de MySQL enviado a Kafka: {mensaje}")
