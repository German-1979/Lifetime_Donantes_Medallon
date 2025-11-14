from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

# Agregar el directorio de Airflow al Python path
sys.path.insert(0, '/opt/airflow')

# Importar las funciones desde scripts
try:
    from scripts.generacion_datos_sinteticos import generar_datos_sinteticos
    from scripts.bronze_layer import procesar_a_bronze  # ← NOMBRE CORRECTO
    from scripts.silver_layer import procesar_a_silver  # ← NOMBRE CORRECTO
    from scripts.gold_layer import procesar_a_gold  # ← NOMBRE CORRECTO
except ImportError as e:
    raise ImportError(f"Error importando módulos: {e}. Verifica que los archivos existan en /opt/airflow/scripts/")

# Argumentos por defecto del DAG
default_args = {
    'owner': 'german',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
}

# Definición del DAG
with DAG(
    dag_id='etl_donaciones_dag',
    default_args=default_args,
    description='Pipeline ETL diario de donaciones (Synthetic → Bronze → Silver → Gold)',
    schedule_interval=timedelta(minutes=3),  # cada 3 minutos
    start_date=datetime(2025, 11, 13),
    catchup=False,
    tags=['donaciones', 'etl', 'airflow'],
) as dag:

    # Tarea 1: Generar datos sintéticos
    generar_datos = PythonOperator(
        task_id='generar_datos_sinteticos',
        python_callable=generar_datos_sinteticos
    )

    # Tarea 2: Procesar capa Bronze
    bronze_task = PythonOperator(
        task_id='procesar_bronze',
        python_callable=procesar_a_bronze
    )

    # Tarea 3: Procesar capa Silver
    silver_task = PythonOperator(
        task_id='procesar_silver',
        python_callable=procesar_a_silver
    )

    # Tarea 4: Procesar capa Gold
    gold_task = PythonOperator(
        task_id='procesar_gold',
        python_callable=procesar_a_gold
    )

    # Flujo de ejecución
    generar_datos >> bronze_task >> silver_task >> gold_task