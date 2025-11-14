import pandas as pd
import numpy as np
import os
from datetime import datetime

def generar_datos_sinteticos():
    """
    Genera un dataset sintético de donaciones mensuales con fugas simuladas
    y lo guarda como 'datos_donantes_sinteticos.csv' en la carpeta /layer/raw/.
    """
    SEMILLA = 42
    np.random.seed(SEMILLA)

    SOCIOS_MENSUALES = 1000
    TASA_FUGA_MENSUAL = 0.02

    # Periodo de análisis: Junio 2023 - Mayo 2025
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

    # Métodos de pago y efectividad
    metodos_pago_config = {
        'Cuenta Corriente': {'probabilidad': 0.12, 'efectividad': 0.97},
        'Tarjeta Crédito': {'probabilidad': 0.10, 'efectividad': 0.93},
        'Cuenta Vista': {'probabilidad': 0.18, 'efectividad': 0.85},
        'Cuenta Rut': {'probabilidad': 0.60, 'efectividad': 0.70}
    }
    metodos_pago = list(metodos_pago_config.keys())
    probabilidades_metodos = [metodos_pago_config[m]['probabilidad'] for m in metodos_pago]

    estrategias = ['Face to Face', 'Telemarketing']
    probabilidades_estrategias = [0.80, 0.20]

    efectividad_promedio = sum(
        metodos_pago_config[m]['efectividad'] * metodos_pago_config[m]['probabilidad']
        for m in metodos_pago
    )
    print(f"Efectividad promedio ponderada: {efectividad_promedio * 100:.2f}%")

    # Estructuras de datos
    socios_info = {}
    socios_activos = set()
    socios_fugados = {}
    registros_por_donante = {}
    id_donante_counter = 1

    # Generar datos mes a mes
    for fecha_mes in meses:
        # Nuevos socios
        for _ in range(SOCIOS_MENSUALES):
            id_donante = f"D{id_donante_counter:06d}"

            if np.random.random() < 0.85:
                monto_fijo = np.random.choice([8000, 9000, 10000])
            else:
                monto_fijo = np.random.choice(range(10000, 26000, 1000))

            metodo_pago = np.random.choice(metodos_pago, p=probabilidades_metodos)
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

        # Cobro a todos los activos
        for id_donante in sorted(socios_activos):
            info = socios_info[id_donante]
            dia = np.random.randint(1, 29)
            fecha_pago = datetime(fecha_mes.year, fecha_mes.month, dia)
            cobro_exitoso = np.random.random() < info['efectividad']

            monto_donacion = info['monto_fijo'] if cobro_exitoso else 0
            if cobro_exitoso:
                info['dono_alguna_vez'] = True

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

        # Aplicar fugas
        total_activos = len(socios_activos)
        num_fugas = int(total_activos * TASA_FUGA_MENSUAL)
        if num_fugas > 0:
            ids_a_fugar = np.random.choice(sorted(list(socios_activos)), size=num_fugas, replace=False)
            for id_fugado in ids_a_fugar:
                dia_fuga = np.random.randint(1, 29)
                fecha_fuga = datetime(fecha_mes.year, fecha_mes.month, dia_fuga)
                fecha_fuga_str = fecha_fuga.strftime('%Y-%m-%d')

                socios_fugados[id_fugado] = fecha_fuga
                dono_realmente = any(
                    pd.notna(reg['Monto_Donacion']) and reg['Monto_Donacion'] > 0
                    for reg in registros_por_donante[id_fugado]
                )

                if not dono_realmente:
                    for reg in registros_por_donante[id_fugado]:
                        reg['Monto_Donacion'] = None
                        reg['Status_Socio'] = 'Fugado'
                        reg['Fecha_Fuga'] = fecha_fuga_str
                else:
                    for reg in registros_por_donante[id_fugado]:
                        reg['Status_Socio'] = 'Fugado'
                        reg['Fecha_Fuga'] = fecha_fuga_str
                    registros_por_donante[id_fugado][-1]['Monto_Donacion'] = 0
                    registros_por_donante[id_fugado][-1]['Fecha_Pago'] = fecha_fuga_str

                socios_activos.remove(id_fugado)

    # Consolidar registros
    registros = []
    for id_donante in registros_por_donante:
        registros.extend(registros_por_donante[id_donante])

    df = pd.DataFrame(registros)
    df['Fecha_Creacion'] = pd.to_datetime(df['Fecha_Creacion'])
    df['Fecha_Pago'] = pd.to_datetime(df['Fecha_Pago'])
    df['Fecha_Fuga'] = pd.to_datetime(df['Fecha_Fuga'], errors='coerce')
    df['Monto_Donacion'] = pd.to_numeric(df['Monto_Donacion'], errors='coerce')

    df['Año_Mes_Creacion'] = df['Fecha_Creacion'].dt.to_period('M').astype(str)
    df['Año_Mes_Donacion'] = df['Fecha_Pago'].dt.to_period('M').astype(str)
    df['Año_Mes_Fuga'] = df['Fecha_Fuga'].dt.to_period('M').astype(str)

    df = df.sort_values(['Fecha_Pago', 'Id_donante']).reset_index(drop=True)

    # Resumen mensual por mes
    resumen_mensual = (
        df.groupby('Año_Mes_Donacion', as_index=True)
        .agg(
            Total_Donaciones=('Monto_Donacion', lambda x: x[x > 0].sum()),
            Cantidad_Donaciones_Exitosas=('Monto_Donacion', lambda x: (x > 0).sum()),
            Fugados=('Status_Socio', lambda x: (x == 'Fugado').sum()),
            Donantes_Unicos=('Id_donante', 'nunique')
        )
    )

    # Fallos de cobro (monto = 0)
    fallos_cobro = df[(df['Status_Socio'] == 'Activo') & (df['Monto_Donacion'] == 0)].groupby('Año_Mes_Donacion').size()
    resumen_mensual['Fallos_Cobro'] = fallos_cobro
    resumen_mensual['Fallos_Cobro'] = resumen_mensual['Fallos_Cobro'].fillna(0).astype(int)

    # Tasa de éxito
    resumen_mensual['Tasa_Exito_%'] = (
        resumen_mensual['Cantidad_Donaciones_Exitosas'] /
        (resumen_mensual['Cantidad_Donaciones_Exitosas'] + resumen_mensual['Fallos_Cobro']) * 100
    ).round(2)

    print(resumen_mensual)

    # Totales acumulados
    total_donaciones_acumuladas = df['Monto_Donacion'].fillna(0).sum()
    total_registros = len(df)
    total_transacciones_acumuladas = (df['Monto_Donacion'].fillna(0) > 0).sum()
    print(f"\nTotal donaciones acumuladas: {total_donaciones_acumuladas:,}")
    print(f"\nCantidad total de registros generados: {total_registros}")
    print(f"Total transacciones acumuladas (>0): {total_transacciones_acumuladas:,}")

    # Guardar CSV
    SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
    ruta_csv = os.path.join(SCRIPT_DIR, "..", "layer", "raw", "datos_donantes_sinteticos.csv")
    os.makedirs(os.path.join(SCRIPT_DIR, "..", "layer", "raw"), exist_ok=True)
    df.to_csv(ruta_csv, index=False, encoding='utf-8-sig')
    print(f"\nArchivo guardado correctamente en: {ruta_csv}")

    return ruta_csv


if __name__ == "__main__":
    generar_datos_sinteticos()