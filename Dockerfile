FROM apache/airflow:2.9.3-python3.11

# Copiamos requirements
COPY requeriments.txt /opt/airflow/requirements.txt

# Cambiamos a usuario airflow
USER airflow

# Instalamos dependencias sin constraints para evitar conflictos
# O usamos el constraint correcto para 2.9.3
RUN pip install --no-cache-dir -r /opt/airflow/requirements.txt \
    --constraint https://raw.githubusercontent.com/apache/airflow/constraints-2.9.3/constraints-3.11.txt