# Estructura del proyecto

Ruta base: `C:\Users\germa\Desktop\Carpetas\Data_Engineer_Specialist\Lifetime_Donantes_Medallon`

```
├── .env
├── Dockerfile
├── airflow
│   ├── dags
│   │   ├── etl_donaciones_dag.py
├── config
│   ├── airflow.cfg
├── docker-compose.yaml
├── estructura.md
├── estructura.py
├── layer
│   ├── __init__.py
│   ├── bronze
│   │   ├── donantes_bronze.parquet
│   │   ├── donantes_bronze.py
│   ├── gold
│   │   ├── cantidad_personas_gold.parquet
│   │   ├── donantes_gold.py
│   │   ├── suma_montos_gold.parquet
│   ├── raw
│   │   ├── datos_donantes_sinteticos.csv
│   ├── silver
│   │   ├── donantes_silver.parquet
│   │   ├── donantes_silver.py
├── main.py
├── requeriments.txt
├── scripts
│   ├── __init__.py
│   ├── bronze_layer.py
│   ├── generacion_datos_sinteticos.py
│   ├── gold_layer.py
│   ├── silver_layer.py
│   ├── streamlit_dashboard_donantes.py
```
