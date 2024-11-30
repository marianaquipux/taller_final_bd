
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# Definir el script a ejecutar
#script_path = os.path.join(os.getenv("AIRFLOW_HOME"), 'scripts', 'postgres_to_kafka.py')

# Argumentos predeterminados para el DAG
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'cargar_datos_bd',
    default_args=default_args,
    description='Un DAG para cargar datos de csv a bd',
    start_date=datetime(2024, 11, 21),
    catchup=False,
)

bash_command_instruction_listar = "ls -l /opt/airflow/scripts/datos"
tarea1 = BashOperator(
    task_id='listar_archivos_iniciales',
    bash_command=bash_command_instruction_listar,
    dag=dag,
)

bash_command_instruction = "python /opt/airflow/scripts/csv_to_mongo.py"
tarea2_mongo = BashOperator(
    task_id='cargar_a_mongo',
    bash_command=bash_command_instruction,
    dag=dag,
)

bash_command_instruction = "python /opt/airflow/scripts/csv_to_postgres.py"
tarea2_postgres = BashOperator(
    task_id='cargar_a_postgres',
    bash_command=bash_command_instruction,
    dag=dag,
)

bash_command_instruction = "python /opt/airflow/scripts/csv_to_mysql.py"
tarea2_mysql = BashOperator(
    task_id='cargar_a_mysql',
    bash_command=bash_command_instruction,
    dag=dag,
)

tarea3 = BashOperator(
    task_id='listar_archivos_finales',
    bash_command=bash_command_instruction_listar,
    dag=dag,
)


tarea1 >> tarea2_mongo >> tarea2_postgres >> tarea2_mysql >> tarea3