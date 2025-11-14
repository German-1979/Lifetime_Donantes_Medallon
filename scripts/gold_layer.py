import os
import pandas as pd
from datetime import datetime, timezone

def procesar_a_gold(nombre_archivo="donantes_silver.parquet"):
    """
    Procesa los datos desde la capa Silver hacia la capa Gold.
    - Carga el archivo Parquet desde /silver.
    - Calcula montos y cantidad de transacciones por mes relativo.
    - Guarda resultados en /gold y crea archivo indicador.
    - NO retorna nada (compatible con Airflow).
    """
    # -------------------------------
    # CONFIGURACIÓN
    # -------------------------------
    base_dir = os.path.dirname(os.path.abspath(__file__))
    carpeta_silver = os.path.join(base_dir, "..", "layer", "silver")
    carpeta_gold = os.path.join(base_dir, "..", "layer", "gold")
    nombre_archivo = nombre_archivo
    ruta_silver = os.path.join(carpeta_silver, nombre_archivo)

    os.makedirs(carpeta_gold, exist_ok=True)

    # -------------------------------
    # 1. VALIDAR EXISTENCIA DEL ARCHIVO EN SILVER
    # -------------------------------
    if not os.path.exists(ruta_silver):
        raise FileNotFoundError(f"No se encontró el archivo en Silver: {ruta_silver}")
    print(f"✓ Archivo encontrado en Silver: {ruta_silver}")

    # -------------------------------
    # 2. CARGAR ARCHIVO PARQUET
    # -------------------------------
    df_pivot = pd.read_parquet(ruta_silver)
    print(f"✓ Archivo leído correctamente. Registros cargados: {len(df_pivot)}")

    # -------------------------------
    # 3. PROCESAMIENTO GOLD
    # -------------------------------
    months = sorted([col for col in df_pivot.columns if isinstance(col, str) and col[:4].isdigit() and '-' in col])
    entry_idx = df_pivot['Año_Mes_Creacion'].apply(lambda x: months.index(x))

    relative_data, presence_data = [], []

    for idx, row in df_pivot.iterrows():
        start = entry_idx[idx]
        rel = row[months[start:]].tolist()
        relative_data.append(rel)
        pres = [1 if val > 0 else 0 for val in rel]
        presence_data.append(pres)

    max_months = max(len(r) for r in relative_data)
    relative_data_padded = [r + [0]*(max_months-len(r)) for r in relative_data]
    presence_data_padded = [r + [0]*(max_months-len(r)) for r in presence_data]

    df_relative = pd.DataFrame(relative_data_padded)
    df_presence = pd.DataFrame(presence_data_padded)

    suma_montos = df_relative.sum(axis=0)
    cantidad_personas = df_presence.sum(axis=0)

    cols = [f"Mes {i+1}" for i in range(max_months)]
    suma_montos.index = cols
    cantidad_personas.index = cols

    # -------------------------------
    # 4. GUARDAR RESULTADOS EN GOLD
    # -------------------------------
    ruta_salida_montos = os.path.join(carpeta_gold, "suma_montos_gold.parquet")
    ruta_salida_personas = os.path.join(carpeta_gold, "cantidad_personas_gold.parquet")

    suma_montos.to_frame(name="Total_Monto").to_parquet(ruta_salida_montos)
    cantidad_personas.to_frame(name="Cantidad_Transacciones").to_parquet(ruta_salida_personas)

    print(f"✓ Datos Gold guardados en: {carpeta_gold}")

    # -------------------------------
    # 5. CREAR ARCHIVO INDICADOR
    # -------------------------------
    ahora_utc = datetime.now(timezone.utc)
    indicador_py = os.path.join(carpeta_gold, "donantes_gold.py")
    with open(indicador_py, "w", encoding="utf-8") as f:
        f.write("# Archivo indicador para la capa Gold\n")
        f.write(f"# Generado: {ahora_utc.isoformat()}\n")
        f.write("# Contiene: suma_montos_gold.parquet y cantidad_personas_gold.parquet\n")

    print(f"✓ Archivo indicador creado: {indicador_py}")
    print("✅ Proceso Gold finalizado correctamente.\n")
    
    # -------------------------------
    # 6. LOG DE RESUMEN (en lugar de return)
    # -------------------------------
    print(f"📊 Resumen de resultados guardados:")
    print(f"   - Total meses procesados: {max_months}")
    print(f"   - Monto total acumulado: ${suma_montos.sum():,.0f}")
    print(f"   - Total transacciones: {int(cantidad_personas.sum())}")
    print(f"   - Archivos generados:")
    print(f"     • {ruta_salida_montos}")
    print(f"     • {ruta_salida_personas}")
    
    # CRÍTICO: NO retornar nada para compatibilidad con Airflow
    # Los datos están guardados en archivos parquet y pueden ser leídos después


# ============================================
# BLOQUE DE EJECUCIÓN LOCAL (para testing)
# ============================================
if __name__ == "__main__":
    print("🔄 Ejecutando proceso Gold en modo local...")
    procesar_a_gold()
    print("\n✅ Proceso completado. Los resultados están en layer/gold/")