import os
import pandas as pd
from datetime import datetime, timezone
import dataframe_image as dfi

def procesar_a_gold(nombre_archivo="donantes_silver.parquet"):
    """
    Procesa los datos desde la capa Silver hacia la capa Gold.
    - Calcula montos y cantidad de transacciones por mes relativo.
    - Genera resúmenes estilo 'show()'.
    - Guarda resultados en /gold y archivos PNG de resumen.
    """
    # -------------------------------
    # CONFIGURACIÓN
    # -------------------------------
    base_dir = os.path.dirname(os.path.abspath(__file__))
    carpeta_silver = os.path.join(base_dir, "..", "layer", "silver")
    carpeta_gold = os.path.join(base_dir, "..", "layer", "gold")
    ruta_silver = os.path.join(carpeta_silver, nombre_archivo)
    os.makedirs(carpeta_gold, exist_ok=True)

    # -------------------------------
    # VALIDAR ARCHIVO
    # -------------------------------
    if not os.path.exists(ruta_silver):
        raise FileNotFoundError(f"No se encontró el archivo en Silver: {ruta_silver}")
    print(f"✓ Archivo encontrado en Silver: {ruta_silver}")

    df_pivot = pd.read_parquet(ruta_silver)
    print(f"✓ Archivo leído correctamente. Registros cargados: {len(df_pivot)}")

    # -------------------------------
    # MESES Y POSICIÓN DE INICIO
    # -------------------------------
    months = sorted([col for col in df_pivot.columns if isinstance(col, str) and col[:4].isdigit() and '-' in col])
    entry_idx = df_pivot['Año_Mes_Creacion'].apply(lambda x: months.index(x))

    # -------------------------------
    # DATOS RELATIVOS
    # -------------------------------
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

    cols = [f"Mes {i+1}" for i in range(max_months)]
    df_relative.columns = cols
    df_presence.columns = cols

    # Agregar Periodo para mostrar como en PySpark
    df_relative_t = df_relative.sum().reset_index()
    df_relative_t.columns = ["Periodo", "Total_Monto"]

    df_presence_t = df_presence.sum().reset_index()
    df_presence_t.columns = ["Periodo", "Cantidad_Transacciones"]

    # -------------------------------
    # LOG ESTILO SHOW()
    # -------------------------------
    print("\n--- Resumen Gold Montos ---")
    print(df_relative_t.to_string(index=False))
    print(f"Total donaciones acumuladas: {df_relative_t['Total_Monto'].sum():,.0f}")

    print("\n--- Resumen Gold Transacciones (>0) ---")
    print(df_presence_t.to_string(index=False))
    print(f"Total transacciones acumuladas: {df_presence_t['Cantidad_Transacciones'].sum():,.0f}")

    # -------------------------------
    # GUARDAR PARQUET
    # -------------------------------
    ruta_salida_montos = os.path.join(carpeta_gold, "suma_montos_gold.parquet")
    ruta_salida_trans = os.path.join(carpeta_gold, "cantidad_personas_gold.parquet")
    df_relative_t.to_parquet(ruta_salida_montos, index=False)
    df_presence_t.to_parquet(ruta_salida_trans, index=False)

    # -------------------------------
    # GUARDAR PNG
    # -------------------------------
    ruta_png_montos = os.path.join(carpeta_gold, "suma_montos_gold.png")
    ruta_png_trans = os.path.join(carpeta_gold, "cantidad_personas_gold.png")
    dfi.export(df_relative_t, ruta_png_montos, max_cols=-1)
    dfi.export(df_presence_t, ruta_png_trans, max_cols=-1)

    # -------------------------------
    # ARCHIVO INDICADOR
    # -------------------------------
    ahora_utc = datetime.now(timezone.utc)
    indicador_py = os.path.join(carpeta_gold, "donantes_gold.py")
    with open(indicador_py, "w", encoding="utf-8") as f:
        f.write("# Archivo indicador para la capa Gold\n")
        f.write(f"# Generado: {ahora_utc.isoformat()}\n")
        f.write("# Contiene: suma_montos_gold.parquet, cantidad_personas_gold.parquet y PNGs\n")
    print(f"✓ Archivo indicador creado: {indicador_py}")

    print("\n✅ Proceso Gold finalizado correctamente.\n")
    print(f"Archivos generados en {carpeta_gold}:")
    print(f" - {ruta_salida_montos}")
    print(f" - {ruta_salida_trans}")
    print(f" - {ruta_png_montos}")
    print(f" - {ruta_png_trans}")


# =======================
# EJECUCIÓN LOCAL
# =======================
if __name__ == "__main__":
    print("🔄 Ejecutando proceso Gold en modo local...")
    procesar_a_gold()
    print("\n✅ Proceso completado. Los resultados están en layer/gold/")