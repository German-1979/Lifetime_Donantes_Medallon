import os
import pandas as pd
from datetime import datetime, timezone

def procesar_a_silver(nombre_archivo="donantes_bronze.parquet"):
    """
    Procesa los datos desde Bronze hacia Silver con pivot mensual.
    Calcula totales acumulados y transacciones efectivas (>0).
    Retorna el DataFrame pivot y un resumen mensual consistente.
    """

    # -------------------------------
    # CONFIGURACIÓN
    # -------------------------------
    base_dir = os.path.dirname(os.path.abspath(__file__))
    carpeta_bronze = os.path.join(base_dir, "..", "layer", "bronze")
    carpeta_silver = os.path.join(base_dir, "..", "layer", "silver")
    ruta_bronze = os.path.join(carpeta_bronze, nombre_archivo)

    os.makedirs(carpeta_silver, exist_ok=True)

    # -------------------------------
    # 1. VALIDAR EXISTENCIA DEL ARCHIVO
    # -------------------------------
    if not os.path.exists(ruta_bronze):
        raise FileNotFoundError(f"No se encontró el archivo en Bronze: {ruta_bronze}")
    print(f"✓ Archivo encontrado en Bronze: {ruta_bronze}")

    # -------------------------------
    # 2. LECTURA DEL ARCHIVO PARQUET
    # -------------------------------
    df_silver = pd.read_parquet(ruta_bronze)
    print(f"✓ Archivo leído correctamente. Registros cargados: {len(df_silver)}")

    # -------------------------------
    # 2b. FILTRAR REGISTROS SIN FECHA DE DONACIÓN
    # -------------------------------
    registros_originales = len(df_silver)
    df_silver = df_silver[df_silver['Año_Mes_Donacion'].notna()].copy()
    registros_filtrados = len(df_silver)
    
    if registros_originales != registros_filtrados:
        print(f"✓ Filtrados {registros_originales - registros_filtrados} registros sin Año_Mes_Donacion (NaT)")

    # ----------------------------------------------------------
    # 3. TRANSFORMACIÓN - ELIMINAR ID FUGADOS Y DONACION + PIVOT
    # ----------------------------------------------------------
    fecha_pago_none = df_silver[df_silver['Fecha_Pago'].isna()]
    df_silver = df_silver.drop(fecha_pago_none.index)

    df_silver['Fecha_Creacion'] = pd.to_datetime(df_silver['Fecha_Creacion'])
    df_silver['Fecha_Pago'] = pd.to_datetime(df_silver['Fecha_Pago'])

    df_pivot_silver = df_silver.pivot_table(
        index=['Id_donante', 'Método_Pago', 'Estrategia', 'Status_Socio', 'Año_Mes_Creacion'],
        columns='Año_Mes_Donacion',
        values='Monto_Donacion',
        aggfunc='sum',
        fill_value=0
    ).reset_index()

    # Quitar nombre de columnas jerárquicas
    df_pivot_silver.columns.name = None
    
    # Eliminar columna NaT si existe en el pivot
    columnas_nat = [col for col in df_pivot_silver.columns if pd.isna(col)]
    if columnas_nat:
        df_pivot_silver = df_pivot_silver.drop(columns=columnas_nat)
        print(f"✓ Eliminadas {len(columnas_nat)} columnas NaT del pivot")
    
    df_pivot_silver = df_pivot_silver.sort_values(by='Id_donante').reset_index(drop=True)

    # -------------------------------
    # 3b. PRINT DE INSPECCIÓN
    # -------------------------------
    print("\n--- Primeros 10 registros de df_pivot_silver ---")
    print(df_pivot_silver.head(10))
    print("\n--- Columnas y tipos ---")
    print(df_pivot_silver.dtypes)

    # -------------------------------
    # 4. GUARDAR PARQUET EN SILVER
    # -------------------------------
    ruta_salida = os.path.join(carpeta_silver, "donantes_silver.parquet")
    ruta_salida_pivot = os.path.join(carpeta_silver, "donantes_silver_pivot.parquet")
    df_silver.to_parquet(ruta_salida, index=False)
    df_pivot_silver.to_parquet(ruta_salida_pivot, index=False)
    print(f"\n✓ Datos procesados y guardados en: {ruta_salida}")
    print(f"\n✓ Datos procesados y guardados en: {ruta_salida_pivot}")

    # -------------------------------
    # 5. ARCHIVO INDICADOR
    # -------------------------------
    indicador_py = os.path.join(carpeta_silver, "donantes_silver.py")
    ahora_utc = datetime.now(timezone.utc)
    with open(indicador_py, "w", encoding="utf-8") as f:
        f.write("# Archivo indicador para la capa Silver\n")
        f.write(f"# Generado: {ahora_utc.isoformat()}\n")
        f.write("# Contiene: donantes_silver.parquet (datos limpios y pivot mensuales)\n")
    print(f"✓ Archivo indicador creado: {indicador_py}")

    # -------------------------------
    # 6. RESUMEN MENSUAL
    # -------------------------------
    meses = [c for c in df_pivot_silver.columns if c not in ['Id_donante', 'Método_Pago', 'Estrategia', 'Status_Socio', 'Año_Mes_Creacion']]
    
    # Filtrar columnas NaT si existen
    meses = [m for m in meses if pd.notna(m)]
    
    resumen_mensual = pd.DataFrame(index=meses)
    resumen_mensual['Total_Donaciones'] = df_pivot_silver[meses].sum()
    resumen_mensual['Cantidad_Donaciones_Exitosas'] = (df_pivot_silver[meses] > 0).sum()
    resumen_mensual['Tasa_Exito_%'] = 100  # Silver no tiene info de fallos de cobro, solo pivot positivo

    print("\n--- RESUMEN MENSUAL EN SILVER ---")
    print(resumen_mensual)

    # -------------------------------
    # 7. TOTALES ACUMULADOS
    # -------------------------------
    total_donaciones = resumen_mensual['Total_Donaciones'].sum()
    total_registros = registros_filtrados  # Usar registros después del filtro
    total_transacciones = resumen_mensual['Cantidad_Donaciones_Exitosas'].sum()

    print("\n--- TOTALES ACUMULADOS EN SILVER ---")
    print(f"Suma total Monto_Donacion: {total_donaciones}")
    print(f"\nCantidad total de registros generados: {total_registros}")
    print(f"Total transacciones (>0): {total_transacciones}")

    return df_pivot_silver, resumen_mensual

if __name__ == "__main__":
    procesar_a_silver()