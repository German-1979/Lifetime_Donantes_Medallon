import os
import pandas as pd
from datetime import datetime, timezone

def procesar_a_bronze(nombre_archivo="datos_donantes_sinteticos.csv"):
    """
    Carga el CSV desde /raw y lo transforma a la capa Bronze (Parquet),
    creando además un archivo indicador para trazabilidad en Airflow.
    Retorna el DataFrame cargado.
    """

    # -------------------------------
    # CONFIGURACIÓN
    # -------------------------------
    base_dir = os.path.dirname(os.path.abspath(__file__))                     
    proyecto_dir = os.path.dirname(base_dir)                                  
    carpeta_raw = os.path.join(proyecto_dir, "layer", "raw")                  
    carpeta_bronze = os.path.join(proyecto_dir, "layer", "bronze")            
    archivo = os.path.join(carpeta_raw, nombre_archivo)

    os.makedirs(carpeta_bronze, exist_ok=True)

    # -------------------------------
    # VALIDAR EXISTENCIA DEL ARCHIVO
    # -------------------------------
    if not os.path.exists(archivo):
        raise FileNotFoundError(f"No se encontró el archivo origen: {archivo}")
    print(f"✓ Archivo encontrado en: {archivo}")

    # -------------------------------
    # CARGAR CSV
    # -------------------------------
    df_bronze = pd.read_csv(archivo, encoding="utf-8-sig")
    print(f"✓ Archivo leído correctamente. Registros cargados: {len(df_bronze)}")

    # -------------------------------
    # GUARDAR PARQUET
    # -------------------------------
    ruta_salida = os.path.join(carpeta_bronze, "donantes_bronze.parquet")
    df_bronze.to_parquet(ruta_salida, index=False)
    print(f"✓ Datos guardados en formato Parquet en: {ruta_salida}")

    # -------------------------------
    # ARCHIVO INDICADOR
    # -------------------------------
    indicador_py = os.path.join(carpeta_bronze, "donantes_bronze.py")
    ahora_utc = datetime.now(timezone.utc)
    with open(indicador_py, "w", encoding="utf-8") as f:
        f.write("# Archivo indicador para la capa Bronze\n")
        f.write(f"# Generado: {ahora_utc.isoformat()}\n")
        f.write("# Contiene: donantes_bronze.parquet (datos crudos procesados)\n")
    print(f"✓ Archivo indicador creado: {indicador_py}")

    # -------------------------------
    # TOTALES ACUMULADOS (solo Monto_Donacion > 0)
    # -------------------------------
    transacciones_efectivas = df_bronze[df_bronze['Monto_Donacion'] > 0]
    total_donaciones = transacciones_efectivas['Monto_Donacion'].sum(skipna=True)
    total_registros = len(df_bronze)
    total_transacciones = len(transacciones_efectivas)
    total_socios_unicos = df_bronze['Id_donante'].nunique()

    print("\n--- RESUMEN BRONZE ACUMULADO ---")
    print(f"Suma total Monto_Donacion: {total_donaciones}")
    print(f"\nCantidad total de registros generados: {total_registros}")
    print(f"Total transacciones (>0): {total_transacciones}")
    print(f"Socios únicos: {total_socios_unicos}")

    return df_bronze


if __name__ == "__main__":
    procesar_a_bronze()