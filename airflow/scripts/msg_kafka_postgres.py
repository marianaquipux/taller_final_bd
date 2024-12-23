from airflow.hooks.postgres_hook import PostgresHook
import json
from kafka import KafkaProducer, KafkaAdminClient
from kafka.admin import NewTopic
import datetime

def verificar_y_crear_tabla_conteos_postgres():
    try:
        conexion_postgres = PostgresHook(postgres_conn_id="conexion_postgres")
        consulta_crear_tabla = """
            CREATE TABLE IF NOT EXISTS public.conteos (
                id SERIAL PRIMARY KEY,
                base_datos VARCHAR(255) NOT NULL,
                tabla VARCHAR(255) NOT NULL,
                cantidad_registros INTEGER NOT NULL,
                ultima_actualizacion TIMESTAMP DEFAULT NOW()
            );
        """
        conexion_postgres.run(consulta_crear_tabla)
        print("Tabla 'conteos' verificada o creada exitosamente.")
    except Exception as e:
        print(f"Error al verificar o crear la tabla 'conteos': {e}")
        raise

def obtener_registros_nuevos_postgres(**kwargs):
    try:
        verificar_y_crear_tabla_conteos_postgres()

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
    
    except Exception as e:
        print(f"Error al obtener registros nuevos de PostgreSQL: {e}")
        raise

def enviar_a_kafka_postgres(**kwargs):
    try:
        bootstrap_servers = 'kafka1:9092'
        topico = 'postgres'
        
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
        
        registros_nuevos = kwargs['ti'].xcom_pull(key='registros_nuevos', task_ids='obtener_registros_nuevos_postgres')
        print(registros_nuevos)

        for registro in registros_nuevos:
            productor_kafka.send(topico, registro)
        
        productor_kafka.flush()
        print(f"Se enviaron {len(registros_nuevos)} registros al tópico '{topico}'.")
    except Exception as e:
        print(f"Error al enviar registros a Kafka: {e}")
        raise

    
def actualizar_conteos_postgres(**kwargs):
    try:
        verificar_y_crear_tabla_conteos_postgres()

        conexion_postgres = PostgresHook(postgres_conn_id="conexion_postgres")
    
        consulta_total_registros = "SELECT COUNT(*) FROM medicamentos;"
        total_registros = conexion_postgres.get_first(consulta_total_registros)[0]
        
        insertar_conteo = """
            INSERT INTO conteos (base_datos, tabla, cantidad_registros, ultima_actualizacion)
            VALUES ('postgres', 'medicamentos', %s, NOW());
        """
        conexion_postgres.run(insertar_conteo, parameters=(total_registros,))

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
            "base_datos": "postgres",
            "tabla": "medicamentos",
            "cantidad_registros": total_registros,
            "ultima_actualizacion": datetime.utcnow().isoformat()
        }

        productor_kafka.send(topic, mensaje)
        productor_kafka.flush()

        print(f"Conteo de PostgreSQL enviado a Kafka: {mensaje}")
    except Exception as e:
        print(f"Error al actualizar el conteo en PostgreSQL: {e}")