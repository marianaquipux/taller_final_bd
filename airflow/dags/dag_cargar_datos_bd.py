
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# Definir el script a ejecutar
#script_path = os.path.join(os.getenv("AIRFLOW_HOME"), 'scripts', 'postgres_to_kafka.py')

# Definir los argumentos predeterminados para el DAG
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Definir el DAG
dag = DAG(
    'ejecutar_script_python',
    default_args=default_args,
    description='Un DAG para ejecutar un script de Python cada 5 minutos',
    schedule_interval='*/5 * * * *',  # Ejecutar cada 5 minutos
    start_date=datetime(2024, 11, 21),
    catchup=False,
)

# Definir el comando bash para ejecutar el script Python
bash_command_instruction = "python /opt/airflow/scripts/postgres_to_kafka.py"

# Crear el BashOperator para ejecutar el script Python
t1 = BashOperator(
    task_id='ejecutar_python_script',
    bash_command=bash_command_instruction,
    dag=dag,
)
