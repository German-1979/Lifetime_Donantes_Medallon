# Estructura del proyecto

Ruta base: `C:\Users\germa\Desktop\Carpetas\Data_Engineer_Specialist\Lifetime_Donantes_Medallon`

```
├── .env
├── .gitignore
├── Dockerfile
├── README.md
├── airflow
│   ├── dags
│   │   ├── etl_donaciones_dag.py
├── config
│   ├── airflow.cfg
├── docker-compose.yaml
├── estructura.py
├── layer
│   ├── __init__.py
│   ├── bronze
│   │   ├── donantes_bronze.parquet
│   │   ├── donantes_bronze.py
│   ├── gold
│   │   ├── cantidad_personas_gold.parquet
│   │   ├── cantidad_personas_gold.png
│   │   ├── donantes_gold.py
│   │   ├── suma_montos_gold.parquet
│   │   ├── suma_montos_gold.png
│   ├── raw
│   │   ├── datos_donantes_sinteticos.csv
│   ├── silver
│   │   ├── donantes_silver.parquet
│   │   ├── donantes_silver.py
│   │   ├── donantes_silver_pivot.parquet
├── main.py
├── requeriments.txt
├── scripts
│   ├── .ipynb_checkpoints
│   │   ├── bronze_layer-checkpoint.ipynb
│   │   ├── bronze_layer-checkpoint.py
│   │   ├── generacion_datos_sinteticos-checkpoint.ipynb
│   │   ├── gold_layer-checkpoint.ipynb
│   │   ├── gold_layer-checkpoint.py
│   │   ├── silver_layer-checkpoint.ipynb
│   ├── __init__.py
│   ├── bronze_layer.py
│   ├── generacion_datos_sinteticos.py
│   ├── gold_layer.py
│   ├── silver_layer.py
│   ├── streamlit_dashboard.py
```
