import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import streamlit as st

# -------------------------------
# CONFIGURACIÓN DE RUTAS
# -------------------------------
base_dir = os.path.dirname(os.path.abspath(__file__))
carpeta_silver = os.path.join(base_dir, "..", "layer", "silver")
ruta_silver = os.path.join(carpeta_silver, "donantes_silver.parquet")

st.set_page_config(page_title="Análisis de Cohortes", layout="wide")
st.title("📊 Análisis Comparativo de Cohortes")
st.markdown("### Face to Face vs Telemarketing - 12 Meses de Seguimiento")
st.markdown("**Cohorte 1:** 2023-06 a 2024-05 | **Cohorte 2:** 2024-06 a 2025-05")
st.markdown("---")

if not os.path.exists(ruta_silver):
    st.error(f"❌ Archivo Silver no encontrado: {ruta_silver}")
    st.stop()

@st.cache_data
def cargar_datos(ruta):
    df = pd.read_parquet(ruta)
    # Asegurar que columnas de mes sean float
    for col in df.columns[5:]:
        df[col] = df[col].astype(float)
    return df

df = cargar_datos(ruta_silver)

# -------------------------------
# DEFINIR COHORTES POR COLUMNAS
# -------------------------------
cohortes = {
    "C1": [f"{y}-{m:02d}" for y in range(2023,2024+1) for m in range(6,13)] + [f"2024-{m:02d}" for m in range(1,6)],
    "C2": [f"2024-{m:02d}" for m in range(6,13)] + [f"2025-{m:02d}" for m in range(1,6)]
}

# Limitar solo a columnas que existen en el df
for k,v in cohortes.items():
    cohortes[k] = [c for c in v if c in df.columns]

# -------------------------------
# FUNCIONES PARA METRICAS
# -------------------------------
def calcular_metricas(df, columnas):
    """
    Devuelve dos dataframes:
    - transacciones >0 por columna (mes)
    - suma de donaciones por columna (mes)
    Agrupado por Estrategia.
    """
    # Transacciones >0 por columna
    transacciones = (df[columnas] > 0).groupby(df['Estrategia']).sum()
    # Donaciones por columna
    donaciones = df[columnas].groupby(df['Estrategia'])[columnas].sum()
    return transacciones, donaciones


# -------------------------------
# CALCULAR Y GRAFICAR
# -------------------------------
# ------------------------------- 
# CALCULAR Y GRAFICAR
# -------------------------------
sns.set(style="whitegrid", palette="muted", font_scale=1.1)
fig, axes = plt.subplots(2,2, figsize=(15,10))
estrategias = ['Face to Face','Telemarketing']

for i, estr in enumerate(estrategias):
    for coh_label, cols in cohortes.items():
        df_estr = df[df['Estrategia']==estr]
        # Transacciones reales por mes (>0)
        trans = (df_estr[cols] > 0).sum()
        # Donaciones reales por mes
        don = df_estr[cols].sum()
        mes_labels = [f"Mes {k+1}" for k in range(len(cols))]

        # Graficar donaciones
        axes[0,i].plot(mes_labels, don, marker='o', label=f"{coh_label}")
        axes[0,i].set_title(f"💰 Donaciones - {estr}")
        axes[0,i].set_ylabel("Monto ($)")
        axes[0,i].legend()
        axes[0,i].grid(True)

        # Graficar transacciones
        axes[1,i].plot(mes_labels, trans, marker='o', label=f"{coh_label}")
        axes[1,i].set_title(f"👥 Transacciones - {estr}")
        axes[1,i].set_ylabel("Cantidad")
        axes[1,i].legend()
        axes[1,i].grid(True)

plt.xticks(rotation=45)
plt.tight_layout()
st.pyplot(fig)

plt.xticks(rotation=45)
plt.tight_layout()
st.pyplot(fig)

# -------------------------------
# TABLAS RESUMEN
# -------------------------------
st.markdown("## 📋 Resumen Totales 12 Meses")
resumen = []
for coh_label, cols in cohortes.items():
    for estr in estrategias:
        df_estr = df[df['Estrategia']==estr]
        total_don = df_estr[cols].sum().sum()
        total_trans = (df_estr[cols]>0).sum().sum()
        resumen.append({'Estrategia':estr,'Cohorte':coh_label,'Donaciones ($M)':round(total_don/1_000_000,2),'Transacciones':total_trans})

resumen_df = pd.DataFrame(resumen)
st.dataframe(resumen_df, use_container_width=True)