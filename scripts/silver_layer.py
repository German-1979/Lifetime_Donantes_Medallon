import os
import pandas as pd
from datetime import datetime, timezone

def procesar_a_silver(nombre_archivo="donantes_bronze.parquet"):
    """
    Procesa los datos desde la capa Bronze hacia la capa Silver.
    - Lee el archivo Parquet desde /bronze.
    - Valida existencia del archivo.
    - Crea la carpeta /silver si no existe.
    - Muestra muestra de datos y tipos.
    - Guarda el resultado como donantes_silver.parquet.
    - Crea un archivo indicador donantes_silver.py.
    - Retorna el DataFrame procesado.
    """

    # -------------------------------
    # CONFIGURACIÓN
    # -------------------------------
    base_dir = os.path.dirname(os.path.abspath(__file__))
    carpeta_bronze = os.path.join(base_dir, "..", "layer", "bronze")
    carpeta_silver = os.path.join(base_dir, "..", "layer", "silver")
    nombre_archivo = nombre_archivo
    ruta_bronze = os.path.join(carpeta_bronze, nombre_archivo)

    # Crear carpeta silver si no existe
    os.makedirs(carpeta_silver, exist_ok=True)

    # -------------------------------
    # 1. VALIDAR EXISTENCIA DEL ARCHIVO EN BRONZE
    # -------------------------------
    if not os.path.exists(ruta_bronze):
        raise FileNotFoundError(f"No se encontró el archivo en Bronze: {ruta_bronze}")
    print(f"✓ Archivo encontrado en Bronze: {ruta_bronze}")

    # -------------------------------
    # 2. LECTURA DEL ARCHIVO PARQUET
    # -------------------------------
    try:
        df_silver = pd.read_parquet(ruta_bronze)
        print(f"✓ Archivo leído correctamente. Registros cargados: {len(df_silver)}")
    except Exception as e:
        raise Exception(f"Error al leer el archivo Parquet: {e}")

    # -------------------------------
    # 3. MOSTRAR MUESTRA Y TIPOS DE DATOS
    # -------------------------------
    print("\n--- Primeros 10 registros ---")
    print(df_silver.head(10))

    print("\n--- Tipos de datos por columna ---")
    print(df_silver.dtypes)

    # -------------------------------
    # 4. TRANSFORMACIONES 
    # -------------------------------
    df_pivot_silver = df_silver.pivot_table(
    index = ['Id_donante', 'Método_Pago', 'Estrategia', 'Status_Socio', 'Año_Mes_Creacion'], columns='Año_Mes_Donacion', 
        values='Monto_Donacion', aggfunc='sum', fill_value=0).reset_index()

    # Quitar nombre de las columnas para que sean planas
    df_pivot_silver.columns.name = None

    # Ordenar por Id_donante
    df_pivot_silver = df_pivot_silver.sort_values(by='Id_donante').reset_index(drop=True)

    print("\n--- Primeros 10 registros de los datos pivoteados ---")
    print(df_pivot_silver.head(10))

    # -------------------------------
    # 5. GUARDAR PARQUET EN SILVER
    # -------------------------------
    ruta_salida = os.path.join(carpeta_silver, "donantes_silver.parquet")
    try:
        df_pivot_silver.to_parquet(ruta_salida, index=False)
        print(f"\n✓ Datos procesados y guardados en: {ruta_salida}")
    except Exception as e:
        raise Exception(f"Error al guardar archivo en Silver: {e}")

    # -------------------------------
    # 6. CREAR ARCHIVO INDICADOR .py EN SILVER
    # -------------------------------

    # datetime con zona horaria UTC, sin usar utcnow()
    ahora_utc = datetime.now(timezone.utc)
    indicador_py = os.path.join(carpeta_silver, "donantes_silver.py")
    try:
        with open(indicador_py, "w", encoding="utf-8") as f:
            f.write("# Archivo indicador para la capa Silver\n")
            f.write(f"# Generado: {ahora_utc.isoformat()}\n")
            f.write("# Contiene: donantes_silver.parquet (datos limpios y listos para análisis)\n")
        print(f"✓ Archivo indicador creado: {indicador_py}")
    except Exception as e:
        raise Exception(f"Error al crear el archivo indicador en Silver: {e}")

    # -------------------------------
    # 7. LOG FINAL
    # -------------------------------
    print("\n--- PROCESO SILVER FINALIZADO ---")
    print(f"Registros finales: {len(df_pivot_silver)}")
    print(f"Columnas: {list(df_pivot_silver.columns)}")

    return df_pivot_silver

# Permite ejecución directa del script
if __name__ == "__main__":
    procesar_a_silver()