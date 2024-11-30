from airflow import DAG
from airflow.sensors.sql_sensor import SqlSensor
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from scripts.msg_kafka_postgres import obtener_registros_nuevos, enviar_a_kafka, actualizar_conteos


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

def flujo_postgres():
    """Flujo de orquestación para PostgreSQL."""
    registros_nuevos = obtener_registros_nuevos()
    enviar_a_kafka(registros_nuevos, topico="postgres")
    actualizar_conteos()


verificar_datos_nuevos = SqlSensor(
    task_id='verificar_datos_nuevos',
    conn_id='conexion_postgres',
    sql="""
        SELECT COUNT(*) > (
            SELECT COALESCE(SUM(cantidad_registros), 0)
            FROM conteos
            WHERE tabla = 'medicamentos'
        )
        FROM medicamentos;
    """,
    mode='poke',
    timeout=300,
    poke_interval=30,
    dag=dag,
)

obtener_nuevos_registros = PythonOperator(
    task_id='obtener_registros_nuevos',
    python_callable=obtener_registros_nuevos,
    provide_context=True,
    dag=dag,
)

enviar_datos_a_kafka = PythonOperator(
    task_id='enviar_datos_a_kafka',
    python_callable=enviar_a_kafka,
    provide_context=True,
    dag=dag,
)

actualizar_tabla_conteos = PythonOperator(
    task_id='actualizar_tabla_conteos',  # Cuarta tarea
    python_callable=actualizar_conteos,
    provide_context=True,
    dag=dag,
)

verificar_datos_nuevos >> obtener_nuevos_registros >> enviar_datos_a_kafka >> actualizar_tabla_conteos