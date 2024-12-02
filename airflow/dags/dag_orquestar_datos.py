from airflow import DAG
from airflow.sensors.sql_sensor import SqlSensor
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from scripts.msg_kafka_postgres import obtener_registros_nuevos_postgres, enviar_a_kafka_postgres, actualizar_conteos_postgres
from scripts.msg_kafka_mysql import obtener_registros_nuevos_mysql, enviar_a_kafka_mysql, actualizar_conteos_mysql
from scripts.msg_kafka_mongo import obtener_registros_nuevos_mongo, enviar_a_kafka_mongo, actualizar_conteos_mongo

import pymongo
from airflow.sensors.python import PythonSensor


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    dag_id='orquestar_datos',
    default_args=default_args,
    description='Orquestar actualización de datos de PostgreSQL, MySQL y Mongo a Kafka.',
    schedule_interval=timedelta(minutes=5),
    start_date=datetime(2024, 11, 21),
    catchup=False,
)

# Flujo postgres-----------------------------------------------------------------------------------

verificar_datos_nuevos_postgres = SqlSensor(
    task_id='verificar_datos_nuevos_postgres',
    conn_id='conexion_postgres',
    sql="""  
        SELECT COUNT(*) 
        FROM conteos c
        WHERE c.id = (
            SELECT id
            FROM conteos
            ORDER BY ultima_actualizacion DESC
            LIMIT 1
        )
        AND c.cantidad_registros != (SELECT COUNT(*) FROM medicamentos);

    """,
    mode='poke',
    timeout=300,
    poke_interval=30,
    dag=dag,
)

obtener_nuevos_registros_postgres = PythonOperator(
    task_id='obtener_registros_nuevos_postgres',
    python_callable=obtener_registros_nuevos_postgres,
    provide_context=True,
    dag=dag,
)

enviar_datos_a_kafka_postgres = PythonOperator(
    task_id='enviar_datos_a_kafka_postgres',
    python_callable=enviar_a_kafka_postgres,
    provide_context=True,
    dag=dag,
)

actualizar_tabla_conteos_postgres = PythonOperator(
    task_id='actualizar_tabla_conteos_posgres',  # Cuarta tarea
    python_callable=actualizar_conteos_postgres,
    provide_context=True,
    dag=dag,
)

# Flujo Mysql -------------------------------------------------------------

verificar_datos_nuevos_mysql = SqlSensor(
    task_id='verificar_datos_nuevos_mysql',
    conn_id='conexion_mysql',
    sql="""  
        SELECT COUNT(*) 
        FROM conteos c
        WHERE c.id = (
            SELECT id
            FROM conteos
            ORDER BY ultima_actualizacion DESC
            LIMIT 1
        )
        AND c.cantidad_registros != (SELECT COUNT(*) FROM extranjeros);

    """,
    mode='poke',
    timeout=300,
    poke_interval=30,
    dag=dag,
)

obtener_nuevos_registros_mysql = PythonOperator(
    task_id='obtener_registros_nuevos_mysql',
    python_callable=obtener_registros_nuevos_mysql,
    provide_context=True,
    dag=dag,
)

enviar_datos_a_kafka_mysql = PythonOperator(
    task_id='enviar_datos_a_kafka_mysql',
    python_callable=enviar_a_kafka_mysql,
    provide_context=True,
    dag=dag,
)

actualizar_tabla_conteos_mysql = PythonOperator(
    task_id='actualizar_tabla_conteos_mysql',
    python_callable=actualizar_conteos_mysql,
    provide_context=True,
    dag=dag,
)

# Flujo mongo --------------------------------------------------------------

def verificar_datos_nuevos_mongo():
    client = pymongo.MongoClient('mongodb://192.168.1.65:27017/')
    db = client['salazarPostgres']
    collection = db['contratos']

    conteo_actual = collection.count_documents({})
    
    conteo_anterior_doc = db['conteos'].find_one({'tabla': 'contratos'}, sort=[('ultima_actualizacion', -1)])
    
    if conteo_anterior_doc:
        conteo_anterior = conteo_anterior_doc.get('cantidad_registros', 0)
    else:
        conteo_anterior = 0
    
    return conteo_actual != conteo_anterior


verificar_datos_nuevos_mongo = PythonSensor(
    task_id='verificar_datos_nuevos_mongo',
    python_callable=verificar_datos_nuevos_mongo,
    poke_interval=30,
    timeout=300,
    mode='poke',
    dag=dag,
)

obtener_nuevos_registros_mongo = PythonOperator(
    task_id='obtener_registros_nuevos_mongo',
    python_callable=obtener_registros_nuevos_mongo,
    provide_context=True,
    dag=dag,
)

enviar_datos_a_kafka_mongo = PythonOperator(
    task_id='enviar_datos_a_kafka_mongo',
    python_callable=enviar_a_kafka_mongo,
    provide_context=True,
    dag=dag,
)

actualizar_tabla_conteos_mongo = PythonOperator(
    task_id='actualizar_tabla_conteos_mongo',
    python_callable=actualizar_conteos_mongo,
    provide_context=True,
    dag=dag,
)

# ---------------------------------------------------------------

# Flujo PostgreSQL
verificar_datos_nuevos_postgres >> obtener_nuevos_registros_postgres >> enviar_datos_a_kafka_postgres >> actualizar_tabla_conteos_postgres

# Flujo MySQL
verificar_datos_nuevos_mysql >> obtener_nuevos_registros_mysql >> enviar_datos_a_kafka_mysql >> actualizar_tabla_conteos_mysql

# Flujo MongoDB
verificar_datos_nuevos_mongo >> obtener_nuevos_registros_mongo >> enviar_datos_a_kafka_mongo >> actualizar_tabla_conteos_mongo
