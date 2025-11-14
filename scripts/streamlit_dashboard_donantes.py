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

# -------------------------------
# CONFIGURACIÓN DE PÁGINA
# -------------------------------
st.set_page_config(page_title="Análisis de Cohortes", layout="wide")

# -------------------------------
# INTERFAZ STREAMLIT
# -------------------------------
st.title("📊 Análisis Comparativo de Cohortes")
st.markdown("### Face to Face vs Telemarketing - 12 Meses de Seguimiento")
st.markdown("**Cohorte 1:** Ingresos 2023-06 a 2024-05 | **Cohorte 2:** Ingresos 2024-06 a 2025-05")
st.markdown("---")

# Validar existencia de archivos
if not os.path.exists(ruta_silver):
    st.error(f"❌ Archivo Silver no encontrado: {ruta_silver}")
    st.stop()

# -------------------------------
# CARGAR Y PROCESAR DATOS
# -------------------------------
@st.cache_data
def cargar_datos(ruta):
    return pd.read_parquet(ruta)

df_silver = cargar_datos(ruta_silver)

# Verificar columnas necesarias
if 'Estrategia' not in df_silver.columns or 'Año_Mes_Creacion' not in df_silver.columns:
    st.error(f"❌ Columnas faltantes. Disponibles: {df_silver.columns.tolist()}")
    st.stop()

# Obtener todas las columnas de meses (formato YYYY-MM)
month_cols = sorted([col for col in df_silver.columns 
                     if isinstance(col, str) and len(col) == 7 and col[:4].isdigit() and '-' in col])

# Definir cohortes por rango de fechas de ingreso
df_cohorte1 = df_silver[
    (df_silver['Año_Mes_Creacion'] >= '2023-06') & 
    (df_silver['Año_Mes_Creacion'] <= '2024-05')
].copy()

df_cohorte2 = df_silver[
    (df_silver['Año_Mes_Creacion'] >= '2024-06') & 
    (df_silver['Año_Mes_Creacion'] <= '2025-05')
].copy()

# -------------------------------
# FUNCIÓN PARA CALCULAR DONACIONES RELATIVAS
# -------------------------------
def calcular_donaciones_relativas(df, max_meses=12):
    """
    Calcula suma de donaciones y transacciones por mes relativo desde ingreso
    """
    if len(df) == 0:
        return pd.Series([0] * max_meses), pd.Series([0] * max_meses)
    
    months = sorted([col for col in df.columns if isinstance(col, str) and len(col) == 7 and col[:4].isdigit() and '-' in col])
    entry_idx = df['Año_Mes_Creacion'].apply(lambda x: months.index(x) if x in months else None)
    
    # Filtrar filas válidas
    df_valid = df[entry_idx.notna()].copy()
    entry_idx = entry_idx[entry_idx.notna()].astype(int)
    
    if len(df_valid) == 0:
        return pd.Series([0] * max_meses), pd.Series([0] * max_meses)
    
    relative_data = []
    presence_data = []
    
    for idx, row in df_valid.iterrows():
        start = entry_idx[idx]
        rel = row[months[start:start+max_meses]].tolist()
        relative_data.append(rel)
        pres = [1 if val > 0 else 0 for val in rel]
        presence_data.append(pres)
    
    # Padding
    max_len = max_meses
    relative_data_padded = [r + [0]*(max_len-len(r)) for r in relative_data]
    presence_data_padded = [p + [0]*(max_len-len(p)) for p in presence_data]
    
    df_relative = pd.DataFrame(relative_data_padded)
    df_presence = pd.DataFrame(presence_data_padded)
    
    suma_montos = df_relative.sum(axis=0)
    cantidad_transacciones = df_presence.sum(axis=0)
    
    return suma_montos, cantidad_transacciones

# -------------------------------
# PROCESAR CADA COMBINACIÓN
# -------------------------------
max_meses = 12

# Debug info en sidebar
with st.sidebar:
    st.subheader("🔍 Debug Info")
    st.write(f"Total registros: {len(df_silver)}")
    st.write(f"Registros Cohorte 1: {len(df_cohorte1)}")
    st.write(f"Registros Cohorte 2: {len(df_cohorte2)}")
    
    count_c1_f2f = len(df_cohorte1[df_cohorte1['Estrategia'] == 'Face to Face'])
    count_c1_tel = len(df_cohorte1[df_cohorte1['Estrategia'] == 'Telemarketing'])
    count_c2_f2f = len(df_cohorte2[df_cohorte2['Estrategia'] == 'Face to Face'])
    count_c2_tel = len(df_cohorte2[df_cohorte2['Estrategia'] == 'Telemarketing'])
    
    st.write("**Distribución:**")
    st.write(f"- Cohorte 1 F2F: {count_c1_f2f}")
    st.write(f"- Cohorte 1 Tel: {count_c1_tel}")
    st.write(f"- Cohorte 2 F2F: {count_c2_f2f}")
    st.write(f"- Cohorte 2 Tel: {count_c2_tel}")

# Calcular métricas
suma_c1_f2f, trans_c1_f2f = calcular_donaciones_relativas(
    df_cohorte1[df_cohorte1['Estrategia'] == 'Face to Face'], max_meses)
suma_c1_tel, trans_c1_tel = calcular_donaciones_relativas(
    df_cohorte1[df_cohorte1['Estrategia'] == 'Telemarketing'], max_meses)
suma_c2_f2f, trans_c2_f2f = calcular_donaciones_relativas(
    df_cohorte2[df_cohorte2['Estrategia'] == 'Face to Face'], max_meses)
suma_c2_tel, trans_c2_tel = calcular_donaciones_relativas(
    df_cohorte2[df_cohorte2['Estrategia'] == 'Telemarketing'], max_meses)

# Etiquetas de mes relativo
mes_labels = [f"Mes {i+1}" for i in range(max_meses)]

sns.set(style="whitegrid", palette="muted", font_scale=1.1)

# -------------------------------
# GRÁFICOS
# -------------------------------
st.markdown("## 📈 Gráficos de Evolución")

# GRÁFICO 1: DONACIONES - Face to Face
col1, col2 = st.columns(2)

with col1:
    st.subheader("💰 Donaciones - Face to Face")
    fig1, ax1 = plt.subplots(figsize=(10, 5))
    
    ax1.plot(mes_labels, suma_c1_f2f.values / 1_000_000, 
             marker='o', linestyle='-', linewidth=2.5, markersize=7, 
             label='Cohorte 1', color='#2E86AB')
    ax1.plot(mes_labels, suma_c2_f2f.values / 1_000_000, 
             marker='o', linestyle='--', linewidth=2.5, markersize=7, 
             label='Cohorte 2', color='#5AB9EA')
    
    ax1.set_xlabel("Mes Relativo", fontsize=10, weight='bold')
    ax1.set_ylabel("Donaciones (Millones $)", fontsize=10, weight='bold')
    ax1.legend(loc='best', fontsize=9)
    ax1.grid(True, alpha=0.3)
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()
    st.pyplot(fig1)

# GRÁFICO 2: DONACIONES - Telemarketing
with col2:
    st.subheader("💰 Donaciones - Telemarketing")
    fig2, ax2 = plt.subplots(figsize=(10, 5))
    
    ax2.plot(mes_labels, suma_c1_tel.values / 1_000_000, 
             marker='s', linestyle='-', linewidth=2.5, markersize=7, 
             label='Cohorte 1', color='#A23B72')
    ax2.plot(mes_labels, suma_c2_tel.values / 1_000_000, 
             marker='s', linestyle='--', linewidth=2.5, markersize=7, 
             label='Cohorte 2', color='#F18F01')
    
    ax2.set_xlabel("Mes Relativo", fontsize=10, weight='bold')
    ax2.set_ylabel("Donaciones (Millones $)", fontsize=10, weight='bold')
    ax2.legend(loc='best', fontsize=9)
    ax2.grid(True, alpha=0.3)
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()
    st.pyplot(fig2)

st.markdown("---")

# GRÁFICO 3: TRANSACCIONES - Face to Face
col3, col4 = st.columns(2)

with col3:
    st.subheader("👥 Transacciones - Face to Face")
    fig3, ax3 = plt.subplots(figsize=(10, 5))
    
    ax3.plot(mes_labels, trans_c1_f2f.values, 
             marker='o', linestyle='-', linewidth=2.5, markersize=7, 
             label='Cohorte 1', color='#2E86AB')
    ax3.plot(mes_labels, trans_c2_f2f.values, 
             marker='o', linestyle='--', linewidth=2.5, markersize=7, 
             label='Cohorte 2', color='#5AB9EA')
    
    ax3.set_xlabel("Mes Relativo", fontsize=10, weight='bold')
    ax3.set_ylabel("Transacciones", fontsize=10, weight='bold')
    ax3.legend(loc='best', fontsize=9)
    ax3.grid(True, alpha=0.3)
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()
    st.pyplot(fig3)

# GRÁFICO 4: TRANSACCIONES - Telemarketing
with col4:
    st.subheader("👥 Transacciones - Telemarketing")
    fig4, ax4 = plt.subplots(figsize=(10, 5))
    
    ax4.plot(mes_labels, trans_c1_tel.values, 
             marker='s', linestyle='-', linewidth=2.5, markersize=7, 
             label='Cohorte 1', color='#A23B72')
    ax4.plot(mes_labels, trans_c2_tel.values, 
             marker='s', linestyle='--', linewidth=2.5, markersize=7, 
             label='Cohorte 2', color='#F18F01')
    
    ax4.set_xlabel("Mes Relativo", fontsize=10, weight='bold')
    ax4.set_ylabel("Transacciones", fontsize=10, weight='bold')
    ax4.legend(loc='best', fontsize=9)
    ax4.grid(True, alpha=0.3)
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()
    st.pyplot(fig4)

st.markdown("---")

# -------------------------------
# TABLAS COMPARATIVAS - MÉTODO ALTERNATIVO
# -------------------------------
st.markdown("## 📋 Resumen Comparativo - Totales (12 meses)")

# Crear un único DataFrame con toda la información
data_completa = {
    'Estrategia': ['Face to Face', 'Face to Face', 'Telemarketing', 'Telemarketing'],
    'Cohorte': ['Cohorte 1', 'Cohorte 2', 'Cohorte 1', 'Cohorte 2'],
    'Donaciones ($M)': [
        suma_c1_f2f.sum() / 1_000_000,
        suma_c2_f2f.sum() / 1_000_000,
        suma_c1_tel.sum() / 1_000_000,
        suma_c2_tel.sum() / 1_000_000
    ],
    'Transacciones': [
        int(trans_c1_f2f.sum()),
        int(trans_c2_f2f.sum()),
        int(trans_c1_tel.sum()),
        int(trans_c2_tel.sum())
    ]
}

df_completo = pd.DataFrame(data_completa)

# Mostrar en dos columnas
col_t1, col_t2 = st.columns(2)

with col_t1:
    st.markdown("### 💰 Donaciones por Estrategia y Cohorte")
    # Tabla pivoteada para donaciones
    df_pivot_don = df_completo.pivot(index='Estrategia', columns='Cohorte', values='Donaciones ($M)')
    df_pivot_don = df_pivot_don.round(2)
    st.dataframe(df_pivot_don, use_container_width=True)

with col_t2:
    st.markdown("### 👥 Transacciones por Estrategia y Cohorte")
    # Tabla pivoteada para transacciones
    df_pivot_trans = df_completo.pivot(index='Estrategia', columns='Cohorte', values='Transacciones')
    st.dataframe(df_pivot_trans, use_container_width=True)

st.markdown("---")

# Tablas adicionales: Comparación directa
st.markdown("### 📊 Comparación Detallada")

col_d1, col_d2 = st.columns(2)

with col_d1:
    st.markdown("#### Face to Face")
    df_f2f = pd.DataFrame({
        'Métrica': ['Donaciones ($M)', 'Transacciones'],
        'Cohorte 1 (2023-06 a 2024-05)': [
            f"${suma_c1_f2f.sum()/1_000_000:,.2f}",
            f"{int(trans_c1_f2f.sum()):,}"
        ],
        'Cohorte 2 (2024-06 a 2025-05)': [
            f"${suma_c2_f2f.sum()/1_000_000:,.2f}",
            f"{int(trans_c2_f2f.sum()):,}"
        ]
    })
    st.dataframe(df_f2f, use_container_width=True, hide_index=True)

with col_d2:
    st.markdown("#### Telemarketing")
    df_tel = pd.DataFrame({
        'Métrica': ['Donaciones ($M)', 'Transacciones'],
        'Cohorte 1 (2023-06 a 2024-05)': [
            f"${suma_c1_tel.sum()/1_000_000:,.2f}",
            f"{int(trans_c1_tel.sum()):,}"
        ],
        'Cohorte 2 (2024-06 a 2025-05)': [
            f"${suma_c2_tel.sum()/1_000_000:,.2f}",
            f"{int(trans_c2_tel.sum()):,}"
        ]
    })
    st.dataframe(df_tel, use_container_width=True, hide_index=True)

st.success("✅ Análisis completado correctamente")