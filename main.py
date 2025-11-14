from scripts.generacion_datos_sinteticos import generar_datos_sinteticos
from scripts.bronze_layer import procesar_a_bronze
from scripts.silver_layer import procesar_a_silver
from scripts.gold_layer import procesar_a_gold

if __name__ == "__main__":
    print("Iniciando pipeline ETL Donaciones (modo local)...")

    generar_datos_sinteticos()
    print("✔ Datos sintéticos generados")

    procesar_a_bronze()
    print("✔ Capa Bronze procesada")

    procesar_a_silver()
    print("✔ Capa Silver procesada")

    procesar_a_gold()
    print("✔ Capa Gold procesada")

    print("✅ Pipeline completo ejecutado correctamente.")