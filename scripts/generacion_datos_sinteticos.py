
import pandas as pd
import numpy as np
import os
from datetime import datetime

def generar_datos_sinteticos():
    """
    Genera un dataset sintético de donaciones mensuales con fugas simuladas
    y lo guarda como 'datos_donantes_sinteticos.csv' en la carpeta /layer/raw/.
    """
    # Configuración de semilla para reproducibilidad
    SEMILLA = 42
    np.random.seed(SEMILLA)

    # Parámetros
    SOCIOS_MENSUALES = 1000
    TASA_FUGA_MENSUAL = 0.02
    PORCENTAJE_NUNCA_DONO_FUGADOS = 0.20  # 20% de fugados nunca donaron
    PORCENTAJE_SI_DONO_FUGADOS = 0.80  # 80% de fugados sí donaron

    # Período de análisis: Enero 2024 - Junio 2025
    fecha_inicio = datetime(2023, 6, 30)
    fecha_fin = datetime(2025, 5, 30)

    # Generar lista de meses
    meses = []
    fecha_actual = fecha_inicio
    while fecha_actual <= fecha_fin:
        meses.append(fecha_actual)
        if fecha_actual.month == 12:
            fecha_actual = datetime(fecha_actual.year + 1, 1, 1)
        else:
            fecha_actual = datetime(fecha_actual.year, fecha_actual.month + 1, 1)

    # Métodos de pago con distribución y efectividad
    metodos_pago_config = {
        'Cuenta Corriente': {'probabilidad': 0.12, 'efectividad': 0.97},
        'Tarjeta Crédito': {'probabilidad': 0.10, 'efectividad': 0.93},
        'Cuenta Vista': {'probabilidad': 0.18, 'efectividad': 0.85},
        'Cuenta Rut': {'probabilidad': 0.60, 'efectividad': 0.70}
    }

    metodos_pago = list(metodos_pago_config.keys())
    probabilidades_metodos = [metodos_pago_config[m]['probabilidad'] for m in metodos_pago]

    # Estrategias disponibles
    estrategias = ['Face to Face', 'Telemarketing']
    probabilidades_estrategias = [0.80, 0.20]

    # Calcular efectividad promedio ponderada
    efectividad_promedio = sum(metodos_pago_config[m]['efectividad'] * metodos_pago_config[m]['probabilidad']
                            for m in metodos_pago)
    print(f"Efectividad promedio ponderada: {efectividad_promedio * 100:.2f}%")

    # Estructura de datos
    socios_info = {}  # Info permanente de cada socio
    socios_activos = set()  # Set de socios activos
    socios_fugados = {}  # {id_donante: fecha_fuga}
    registros_por_donante = {}  # {id_donante: [lista de registros]}
    id_donante_counter = 1

    # Procesar cada mes
    for idx_mes, fecha_mes in enumerate(meses):
        print(f"\nProcesando {fecha_mes.strftime('%Y-%m')}...")

        # 1. INGRESAN 1000 NUEVOS SOCIOS
        nuevos_socios_ids = []
        for _ in range(SOCIOS_MENSUALES):
            id_donante = f"D{id_donante_counter:06d}"
            nuevos_socios_ids.append(id_donante)

            # Asignar monto fijo: distribución entre 8000-10000 (85%) y 11000-25000 (15%)
            if np.random.random() < 0.85:
                # 85% dona entre 8000 y 10000 (múltiplos de 1000)
                monto_fijo = np.random.choice([8000, 9000, 10000])
            else:
                # 15% dona entre 10000 y 25000 (múltiplos de 1000)
                monto_fijo = np.random.choice(range(10000, 26000, 1000))

            # Asignar método de pago según distribución
            metodo_pago = np.random.choice(metodos_pago, p=probabilidades_metodos)

            # Asignar estrategia según distribución
            estrategia = np.random.choice(estrategias, p=probabilidades_estrategias)

            socios_info[id_donante] = {
                'fecha_creacion': fecha_mes,
                'metodo_pago': metodo_pago,
                'estrategia': estrategia,
                'monto_fijo': monto_fijo,
                'efectividad': metodos_pago_config[metodo_pago]['efectividad'],
                'dono_alguna_vez': False
            }
            socios_activos.add(id_donante)
            registros_por_donante[id_donante] = []
            id_donante_counter += 1

        # 2. INTENTAR COBRO A TODOS LOS ACTIVOS (nuevos + históricos)
        # Ordenar para garantizar reproducibilidad
        socios_activos_ordenados = sorted(list(socios_activos))

        for id_donante in socios_activos_ordenados:
            info = socios_info[id_donante]

            # Generar fecha aleatoria dentro del mes
            dia = np.random.randint(1, 29)
            fecha_pago = datetime(fecha_mes.year, fecha_mes.month, dia)

            # Probabilidad de cobro exitoso según efectividad del método de pago
            efectividad = info['efectividad']
            cobro_exitoso = np.random.random() < efectividad

            if cobro_exitoso:
                # Cobro exitoso
                monto_donacion = info['monto_fijo']
                info['dono_alguna_vez'] = True
            else:
                # Cobro falló
                monto_donacion = 0

            registros_por_donante[id_donante].append({
                'Id_donante': id_donante,
                'Método_Pago': info['metodo_pago'],
                'Estrategia': info['estrategia'],
                'Fecha_Creacion': info['fecha_creacion'].strftime('%Y-%m-%d'),
                'Fecha_Pago': fecha_pago.strftime('%Y-%m-%d'),
                'Monto_Donacion': monto_donacion,
                'Status_Socio': 'Activo',
                'Fecha_Fuga': ''
            })

        # 3. APLICAR FUGA DEL 2% SOBRE TODOS LOS ACTIVOS
        total_socios_activos = len(socios_activos)
        num_fugas = int(total_socios_activos * TASA_FUGA_MENSUAL)

        if num_fugas > 0:
            # Convertir a lista ordenada para garantizar reproducibilidad
            socios_activos_lista = sorted(list(socios_activos))
            # Seleccionar socios que se fugan
            ids_a_fugar = np.random.choice(socios_activos_lista, size=num_fugas, replace=False)

            for id_fugado in ids_a_fugar:
                info_fugado = socios_info[id_fugado]

                # Generar fecha de fuga
                dia_fuga = np.random.randint(1, 29)
                fecha_fuga = datetime(fecha_mes.year, fecha_mes.month, dia_fuga)
                fecha_fuga_str = fecha_fuga.strftime('%Y-%m-%d')

                # Registrar fuga
                socios_fugados[id_fugado] = fecha_fuga

                # Verificar si realmente donó alguna vez (revisar el historial real, no el flag)
                dono_realmente = any(reg['Monto_Donacion'] > 0 for reg in registros_por_donante[id_fugado])

                if not dono_realmente:
                    # NUNCA DONÓ: todos los registros con Monto_Donacion = None (se convertirá a NaN)
                    for registro in registros_por_donante[id_fugado]:
                        registro['Monto_Donacion'] = None
                        registro['Status_Socio'] = 'Fugado'
                        registro['Fecha_Fuga'] = fecha_fuga_str
                else:
                    # SÍ DONÓ: actualizar todos los registros a Fugado
                    # El último registro (mes actual) debe tener Monto_Donacion = 0
                    for registro in registros_por_donante[id_fugado]:
                        registro['Status_Socio'] = 'Fugado'
                        registro['Fecha_Fuga'] = fecha_fuga_str

                    # Modificar el último registro para que tenga monto 0
                    if len(registros_por_donante[id_fugado]) > 0:
                        registros_por_donante[id_fugado][-1]['Monto_Donacion'] = 0
                        registros_por_donante[id_fugado][-1]['Fecha_Pago'] = fecha_fuga_str

                # Eliminar de activos (ya no generará más registros)
                socios_activos.remove(id_fugado)

        print(f"  Activos al final del mes: {len(socios_activos)}")
        print(f"  Fugados en este mes: {num_fugas}")
        print(f"  Total fugados acumulados: {len(socios_fugados)}")

    # Consolidar todos los registros
    registros = []
    for id_donante in registros_por_donante:
        registros.extend(registros_por_donante[id_donante])

    # Crear DataFrame
    df = pd.DataFrame(registros)

    # Convertir tipos de datos
    df['Fecha_Creacion'] = pd.to_datetime(df['Fecha_Creacion'])
    df['Fecha_Pago'] = pd.to_datetime(df['Fecha_Pago'])
    df['Fecha_Fuga'] = pd.to_datetime(df['Fecha_Fuga'], errors='coerce')

    # Convertir Monto_Donacion: 'NaN_MARKER' -> NaN, números se mantienen
    df['Monto_Donacion'] = df['Monto_Donacion'].replace('NaN_MARKER', np.nan)
    df['Monto_Donacion'] = pd.to_numeric(df['Monto_Donacion'], errors='coerce')

    # Agregar columnas auxiliares
    df['Año_Mes_Creacion'] = df['Fecha_Creacion'].dt.to_period('M').astype(str)
    df['Año_Mes_Donacion'] = df['Fecha_Pago'].dt.to_period('M').astype(str)
    df['Año_Mes_Fuga'] = df['Fecha_Fuga'].dt.to_period('M').astype(str)

    # Ordenar por fecha de pago
    df = df.sort_values(['Fecha_Pago', 'Id_donante']).reset_index(drop=True)

    # Mostrar estadísticas generales
    print("\n" + "=" * 70)
    print("RESUMEN DE DATOS GENERADOS")
    print("=" * 70)
    print(f"Total de registros: {len(df):,}")
    print(f"Socios únicos totales: {df['Id_donante'].nunique():,}")
    print(f"Socios activos al final: {len(socios_activos):,}")
    print(f"Socios fugados totales: {len(socios_fugados):,}")

    # Distribución de métodos de pago
    print(f"\n{'Distribución por Método de Pago:'}")
    metodos_counts = pd.Series([socios_info[id_d]['metodo_pago'] for id_d in socios_info]).value_counts()
    print(metodos_counts)
    print("\nPorcentajes:")
    print((metodos_counts / metodos_counts.sum() * 100).round(2))

    # Distribución de estrategias
    print(f"\n{'Distribución por Estrategia:'}")
    estrategias_counts = pd.Series([socios_info[id_d]['estrategia'] for id_d in socios_info]).value_counts()
    print(estrategias_counts)
    print("\nPorcentajes:")
    print((estrategias_counts / estrategias_counts.sum() * 100).round(2))

    # Análisis de fugados
    fugados_nunca_donaron_ids = []
    for id_d in socios_fugados:
        dono_en_historial = any(
            isinstance(reg['Monto_Donacion'], (int, float)) and reg['Monto_Donacion'] > 0
            for reg in registros_por_donante[id_d]
        )
        if not dono_en_historial:
            fugados_nunca_donaron_ids.append(id_d)

    fugados_nunca_donaron = len(fugados_nunca_donaron_ids)
    fugados_si_donaron = len(socios_fugados) - fugados_nunca_donaron
    print(f"\n{'Análisis de Fugados:'}")
    print(f"Fugados que NUNCA donaron: {fugados_nunca_donaron} ({fugados_nunca_donaron/len(socios_fugados)*100:.1f}%)")
    print(f"Fugados que SÍ donaron: {fugados_si_donaron} ({fugados_si_donaron/len(socios_fugados)*100:.1f}%)")

    # Resumen mensual
    print("\n" + "=" * 70)
    print("RESUMEN POR MES")
    print("=" * 70)
    resumen_mensual = df.groupby('Año_Mes_Donacion').agg({
        'Monto_Donacion': ['sum', lambda x: (x > 0).sum()],
        'Status_Socio': lambda x: (x == 'Fugado').sum(),
        'Id_donante': 'nunique'
    }).round(2)

    resumen_mensual.columns = ['Total_Donaciones', 'Donaciones_Exitosas', 'Fugados', 'Donantes_Unicos']

    # Calcular fallos de cobro (activos con monto 0)
    fallos_cobro = df[(df['Status_Socio'] == 'Activo') & (df['Monto_Donacion'] == 0)].groupby('Año_Mes_Donacion').size()
    resumen_mensual['Fallos_Cobro'] = fallos_cobro

    # Calcular tasa de éxito
    resumen_mensual['Tasa_Exito_%'] = (resumen_mensual['Donaciones_Exitosas'] /
                                        df.groupby('Año_Mes_Donacion').size() * 100).round(2)

    print(resumen_mensual)

    # Verificación enero 2024
    print("\n" + "=" * 70)
    print("VERIFICACIÓN: ENERO 2024")
    print("=" * 70)
    enero_2024 = df[df['Año_Mes_Donacion'] == '2024-01']
    print(f"Donantes únicos: {enero_2024['Id_donante'].nunique()}")
    print(f"Donaciones exitosas: {(enero_2024['Monto_Donacion'] > 0).sum()}")
    print(f"Cobros fallidos: {(enero_2024['Monto_Donacion'] == 0).sum()}")
    print(f"Fugados sin donación (monto=NaN): {enero_2024['Monto_Donacion'].isna().sum()}")
    print(f"Tasa de éxito: {(enero_2024['Monto_Donacion'] > 0).sum() / len(enero_2024) * 100:.2f}%")


    # obtener el directorio donde está el script actual
    SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

    # construir la ruta al CSV dentro del mismo directorio
    ruta_csv = os.path.join(SCRIPT_DIR, "..", "layer", "raw", "datos_donantes_sinteticos.csv")

    # guardar el archivo asegurando que la carpeta existe
    os.makedirs(SCRIPT_DIR, exist_ok=True)
    df.to_csv(ruta_csv, index=False, encoding='utf-8-sig')

    print(f"\nArchivo guardado correctamente en: {ruta_csv}")

    return ruta_csv

# Permite ejecución directa del script
if __name__ == "__main__":
    generar_datos_sinteticos()