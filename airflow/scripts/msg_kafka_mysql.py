from airflow.hooks.mysql_hook import MySqlHook
from kafka import KafkaProducer
import json

def obtener_registros_nuevos_mysql(**kwargs):
    conexion_mysql = MySqlHook(mysql_conn_id="conexion_mysql")

    consulta_ultimo_conteo_mysql = """
        SELECT cantidad_registros 
        FROM conteos 
        WHERE tabla = 'extranjeros' 
        ORDER BY ultima_actualizacion DESC 
        LIMIT 1;
    """
    ultimo_conteo_mysql = conexion_mysql.get_first(consulta_ultimo_conteo_mysql)
    ultimo_conteo_mysql = ultimo_conteo_mysql[0] if ultimo_conteo_mysql else 0

    consulta_nuevos_registros_mysql = f"SELECT * FROM a_o_mes LIMIT {ultimo_conteo_mysql}, 1000;"  # Ajustar OFFSET si es necesario
    registros_nuevos_mysql = conexion_mysql.get_records(consulta_nuevos_registros_mysql)
    
    kwargs['ti'].xcom_push(key='registros_nuevos_mysql', value=registros_nuevos_mysql)
    return registros_nuevos_mysql

def enviar_a_kafka_mysql(**kwargs):
    productor_kafka = KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    registros_nuevos_mysql = kwargs['ti'].xcom_pull(key='registros_nuevos_mysql', task_ids='obtener_registros_nuevos_mysql')
    topico_mysql = 'mysql'
    for registro in registros_nuevos_mysql:
        productor_kafka.send(topico_mysql, registro)
    productor_kafka.flush()

def actualizar_conteos_mysql(**kwargs):
    conexion_mysql = MySqlHook(mysql_conn_id="conexion_mysql")
    consulta_total_registros_mysql = "SELECT COUNT(*) FROM a_o_mes;"
    total_registros_mysql = conexion_mysql.get_first(consulta_total_registros_mysql)[0]
    
    insertar_conteo_mysql = """
        INSERT INTO conteos (base_datos, tabla, cantidad_registros, ultima_actualizacion)
        VALUES ('mysql', 'a_o_mes', %s, NOW());
    """
    conexion_mysql.run(insertar_conteo_mysql, parameters=(total_registros_mysql,))
