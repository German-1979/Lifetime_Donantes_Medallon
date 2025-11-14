import os
import pandas as pd
from datetime import datetime, timezone

def procesar_a_bronze(nombre_archivo="datos_donantes_sinteticos.csv"):
    """
    Carga el archivo CSV desde /raw y lo transforma a la capa Bronze (Parquet).
    - Verifica existencia del archivo en /raw.
    - Lee el CSV.
    - Guarda una copia en /bronze.
    - Crea un archivo indicador .py (para trazabilidad en Airflow).
    - Retorna el DataFrame cargado.
    """

    # -------------------------------
    # CONFIGURACIÓN
    # -------------------------------
    base_dir = os.path.dirname(os.path.abspath(__file__))                     # /scripts
    proyecto_dir = os.path.dirname(base_dir)                                  # raíz del proyecto
    carpeta_raw = os.path.join(proyecto_dir, "layer", "raw")                  # /layer/raw
    carpeta_bronze = os.path.join(proyecto_dir, "layer", "bronze")            # /layer/bronze
    archivo = os.path.join(carpeta_raw, nombre_archivo)

    # Crear carpeta Bronze si no existe
    os.makedirs(carpeta_bronze, exist_ok=True)

    # -------------------------------
    # 1. VALIDAR EXISTENCIA DEL ARCHIVO
    # -------------------------------
    if not os.path.exists(archivo):
        raise FileNotFoundError(f"No se encontró el archivo origen: {archivo}")
    print(f"✓ Archivo encontrado en: {archivo}")

    # -------------------------------
    # 2. CARGAR DATOS A BRONZE
    # -------------------------------
    try:
        df_bronze = pd.read_csv(archivo, encoding="utf-8-sig")
        print(f"✓ Archivo leído correctamente. Registros cargados: {len(df_bronze)}")
    except Exception as e:
        raise Exception(f"Error al leer el archivo CSV: {e}")

    # -------------------------------
    # 3. GUARDAR COPIA EN BRONZE
    # -------------------------------
    ruta_salida = os.path.join(carpeta_bronze, "donantes_bronze.parquet")
    try:
        df_bronze.to_parquet(ruta_salida, index=False)
        print(f"✓ Datos guardados en formato Parquet en: {ruta_salida}")
    except Exception as e:
        raise Exception(f"Error al guardar archivo en Bronze: {e}")

    # -------------------------------
    # 4. CREAR ARCHIVO INDICADOR
    # -------------------------------
    indicador_py = os.path.join(carpeta_bronze, "donantes_bronze.py")
    try:
        # datetime con zona horaria UTC, sin usar utcnow()
        ahora_utc = datetime.now(timezone.utc)
        with open(indicador_py, "w", encoding="utf-8") as f:
            f.write("# Archivo indicador para la capa Bronze\n")
            f.write(f"# Generado: {ahora_utc.isoformat()}\n")
            f.write("# Contiene: donantes_bronze.parquet (datos crudos procesados)\n")
        print(f"✓ Archivo indicador creado: {indicador_py}")
    except Exception as e:
        raise Exception(f"Error al crear el archivo indicador: {e}")

    # -------------------------------
    # 5. LOG FINAL
    # -------------------------------
    print(f"--- PROCESO BRONZE FINALIZADO ---")
    print(f"Registros cargados: {len(df_bronze)}")
    print(f"Columnas: {list(df_bronze.columns)}")

    return df_bronze

# Permite ejecución directa del script
if __name__ == "__main__":
    procesar_a_bronze()