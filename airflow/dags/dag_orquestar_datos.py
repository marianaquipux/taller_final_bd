from airflow import DAG
from airflow.sensors.sql_sensor import SqlSensor
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from scripts.msg_kafka_postgres import obtener_registros_nuevos, enviar_a_kafka, actualizar_conteos


# Configuración predeterminada del DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Definición del DAG
dag = DAG(
    dag_id='orquestar_datos',
    default_args=default_args,
    description='Orquestar actualización de datos de PostgreSQL, MySQL y Mongo a Kafka.',
    schedule_interval=timedelta(minutes=5),
    start_date=datetime(2024, 11, 21),
    catchup=False,
)

# Función que orquesta el flujo completo
def flujo_postgres():
    """Flujo de orquestación para PostgreSQL."""
    registros_nuevos = obtener_registros_nuevos()
    enviar_a_kafka(registros_nuevos, topico="postgres")
    actualizar_conteos()

# Sensor para tabla "medicamentos" en PostgreSQL
verificar_datos_nuevos = SqlSensor(
    task_id='verificar_datos_nuevos',  # Esta es la primera tarea
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

# Tarea para obtener los nuevos registros de la tabla "medicamentos"
obtener_nuevos_registros = PythonOperator(
    task_id='obtener_registros_nuevos',  # Segunda tarea
    python_callable=obtener_registros_nuevos,
    provide_context=True,
    dag=dag,
)

# Tarea para enviar los datos a Kafka
enviar_datos_a_kafka = PythonOperator(
    task_id='enviar_datos_a_kafka',  # Tercera tarea
    python_callable=enviar_a_kafka,
    provide_context=True,
    dag=dag,
)

# Tarea para actualizar la tabla "conteos" después de enviar los datos
actualizar_tabla_conteos = PythonOperator(
    task_id='actualizar_tabla_conteos',  # Cuarta tarea
    python_callable=actualizar_conteos,
    provide_context=True,
    dag=dag,
)

# Definir el flujo de tareas: asegurar que se ejecuten en orden
verificar_datos_nuevos >> obtener_nuevos_registros >> enviar_datos_a_kafka >> actualizar_tabla_conteos
