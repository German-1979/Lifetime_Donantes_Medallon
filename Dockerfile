FROM apache/airflow:2.9.3-python3.11

# Copiamos requirements
COPY requeriments.txt /opt/airflow/requirements.txt

# Cambiamos a root para instalar paquetes del sistema
USER root

# Instalamos Chromium y dependencias necesarias
RUN apt-get update && apt-get install -y \
    chromium \
    chromium-driver \
    fonts-liberation \
    libnss3 \
    libx11-xcb1 \
    libxcomposite1 \
    libxdamage1 \
    libxrandr2 \
    libgtk-3-0 \
    libgbm1 \
    libasound2 \
    --no-install-recommends && \
    rm -rf /var/lib/apt/lists/*

# Volvemos a usuario airflow
USER airflow

# Instalamos Python requirements con constraints de Airflow
RUN pip install --no-cache-dir -r /opt/airflow/requirements.txt \
    --constraint https://raw.githubusercontent.com/apache/airflow/constraints-2.9.3/constraints-3.11.txt