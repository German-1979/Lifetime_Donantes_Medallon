import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import streamlit as st
import os

# -------------------------------
# CONFIGURACIÓN
# -------------------------------
base_dir = os.path.dirname(os.path.abspath(__file__))
carpeta_silver = os.path.join(base_dir, "..", "layer", "silver")
ruta_silver = os.path.join(carpeta_silver, "donantes_silver.parquet")

st.set_page_config(page_title="Análisis del LifeTime de los Donantes", layout="wide")
st.title("📊 Análisis del LifeTime de los Donantes")

if not os.path.exists(ruta_silver):
    st.error(f"❌ Archivo Silver no encontrado: {ruta_silver}")
    st.stop()

# -------------------------------
# CARGAR DATOS
# -------------------------------
@st.cache_data
def cargar_datos(ruta):
    return pd.read_parquet(ruta).sort_values(['Id_donante', 'Año_Mes_Creacion']).reset_index(drop=True)

df = cargar_datos(ruta_silver)

# -------------------------------
# CÁLCULOS BASE
# -------------------------------
grouped = df.groupby(['Año_Mes_Creacion', 'Año_Mes_Donacion'])
cohorts = grouped.agg({'Id_donante': 'nunique', 'Monto_Donacion': 'sum'})
cohort_size = cohorts['Id_donante'].groupby(level=0).first()
retencion = cohorts['Id_donante'].unstack(0)
retencion_pc = retencion.divide(cohort_size, axis=0)

# -------------------------------
# TABS
# -------------------------------
tab1, tab2, tab3, tab4 = st.tabs([
    "📈 Heatmaps Clásicos", 
    "📊 Métricas Clave", 
    "🔍 Segmentación",
    "⏱️ Lifetime Analysis"
])

# ===============================
# TAB 1: HEATMAPS
# ===============================
with tab1:
    st.header("Heatmaps de Retención")
    
    # Heatmap Porcentaje
    fig1, ax1 = plt.subplots(figsize=(22, 12))
    sns.heatmap(retencion_pc.T, cmap='cividis', annot=True, fmt='.0%',
                annot_kws={'size': 12}, linewidths=0.5, linecolor='white',
                cbar_kws={'shrink': 0.7}, ax=ax1)
    ax1.set_title('LifeTime expresado en Porcentaje', fontsize=20, pad=20)
    b, t = ax1.get_ylim()
    ax1.set_ylim(b+0.5, t-0.5)
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()
    st.pyplot(fig1)
    
    # Heatmap Cantidad
    fig2, ax2 = plt.subplots(figsize=(22, 12))
    sns.heatmap(retencion.T, cmap='magma', annot=True, fmt='.0f',
                annot_kws={'size': 12}, linewidths=0.5, linecolor='white',
                cbar_kws={'shrink': 0.7}, ax=ax2)
    ax2.set_title('LifeTime expresado en Cantidad de Donantes', fontsize=20, pad=20)
    b, t = ax2.get_ylim()
    ax2.set_ylim(b+0.5, t-0.5)
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()
    st.pyplot(fig2)
    
    st.markdown("---")
    
    # Heatmap Ingresos
    st.subheader("💵 Ingresos por Cohorte y Período")
    revenue = df.groupby(['Año_Mes_Creacion', 'Año_Mes_Donacion'])['Monto_Donacion'].sum()
    revenue_matrix = revenue.unstack(0).T / 1_000_000
    
    fig3, ax3 = plt.subplots(figsize=(22, 12))
    sns.heatmap(revenue_matrix, cmap='YlGn', annot=True, fmt='.1f',
                annot_kws={'size': 11}, linewidths=0.5, linecolor='white',
                cbar_kws={'shrink': 0.7, 'label': 'Millones de $'}, ax=ax3)
    ax3.set_title('Ingresos por Cohorte y Período (en Millones de $)', fontsize=20, pad=20)
    b, t = ax3.get_ylim()
    ax3.set_ylim(b+0.5, t-0.5)
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()
    st.pyplot(fig3)

# ===============================
# TAB 2: MÉTRICAS CLAVE
# ===============================
with tab2:
    st.header("Métricas Clave por Cohorte")
    
    # KPIs Generales
    st.subheader("🎯 KPIs Generales")
    col1, col2, col3, col4 = st.columns(4)
    
    total_donantes = df['Id_donante'].nunique()
    df_fugados = df[df['Status_Socio'] == 'Fugado']
    tasa_fuga_global = (df_fugados['Id_donante'].nunique() / total_donantes * 100)
    donacion_promedio = df['Monto_Donacion'].mean()
    ltv_promedio = df.groupby('Id_donante')['Monto_Donacion'].sum().mean()
    
    col1.metric("Total Donantes", f"{total_donantes:,}")
    col2.metric("Tasa de Fuga Global", f"{tasa_fuga_global:.1f}%")
    col3.metric("Donación Promedio", f"${donacion_promedio:,.0f}")
    col4.metric("LTV Promedio", f"${ltv_promedio:,.0f}")
    
    st.markdown("---")
    
    # Tabla Resumen
    st.subheader("📋 Tabla Resumen por Cohorte")
    
    resumen_base = df.groupby('Año_Mes_Creacion').agg({
        'Id_donante': 'nunique',
        'Monto_Donacion': ['sum', 'mean']
    })
    resumen_base.columns = ['Total Donantes', 'Monto Total', 'Donación Promedio']
    
    fugados_por_cohorte = df_fugados.groupby('Año_Mes_Creacion')['Id_donante'].nunique()
    resumen_base['Total Fugados'] = fugados_por_cohorte.reindex(resumen_base.index, fill_value=0).astype(int)
    resumen_base['Tasa de Fuga (%)'] = (resumen_base['Total Fugados'] / resumen_base['Total Donantes'] * 100).round(2)
    resumen_base['LTV Promedio'] = resumen_base['Monto Total'] / resumen_base['Total Donantes']
    
    # Formatear columnas de dinero
    resumen_base['Monto Total'] = resumen_base['Monto Total'].apply(lambda x: f"${x:,.0f}")
    resumen_base['Donación Promedio'] = resumen_base['Donación Promedio'].apply(lambda x: f"${x:,.0f}")
    resumen_base['LTV Promedio'] = resumen_base['LTV Promedio'].apply(lambda x: f"${x:,.0f}")
    
    st.dataframe(resumen_base, use_container_width=True, height=400)
    
    st.markdown("---")
    
    # Gráficos
    col_graf1, col_graf2 = st.columns(2)
    
    with col_graf1:
        st.subheader("💰 Monto Total por Cohorte")
        monto_cohorte = df.groupby('Año_Mes_Creacion')['Monto_Donacion'].sum()
        fig4, ax4 = plt.subplots(figsize=(10, 6))
        ax4.bar(range(len(monto_cohorte)), monto_cohorte.values, color='#2E86AB', alpha=0.8)
        ax4.set_xticks(range(len(monto_cohorte)))
        ax4.set_xticklabels(monto_cohorte.index, rotation=45, ha='right')
        ax4.set_xlabel('Cohorte', fontsize=12)
        ax4.set_ylabel('Monto Total ($)', fontsize=12)
        ax4.set_title('Monto Total por Cohorte', fontsize=14, pad=15)
        ax4.grid(axis='y', alpha=0.3)
        plt.tight_layout()
        st.pyplot(fig4)
    
    with col_graf2:
        st.subheader("👥 Total Donantes por Cohorte")
        donantes_cohorte = df.groupby('Año_Mes_Creacion')['Id_donante'].nunique()
        fig5, ax5 = plt.subplots(figsize=(10, 6))
        ax5.bar(range(len(donantes_cohorte)), donantes_cohorte.values, color='#A23B72', alpha=0.8)
        ax5.set_xticks(range(len(donantes_cohorte)))
        ax5.set_xticklabels(donantes_cohorte.index, rotation=45, ha='right')
        ax5.set_xlabel('Cohorte', fontsize=12)
        ax5.set_ylabel('Total Donantes', fontsize=12)
        ax5.set_title('Total Donantes por Cohorte', fontsize=14, pad=15)
        ax5.grid(axis='y', alpha=0.3)
        plt.tight_layout()
        st.pyplot(fig5)

# ===============================
# TAB 3: SEGMENTACIÓN
# ===============================
with tab3:
    st.header("Análisis por Segmentos")
    
    # Tabla Estrategia (PRIMERA)
    st.subheader("🎯 Comparación por Estrategia")
    
    estrategia_donantes = df.groupby('Estrategia')['Id_donante'].nunique()
    estrategia_monto = df.groupby('Estrategia')['Monto_Donacion'].agg(['sum', 'mean'])
    estrategia_fugados = df_fugados.groupby('Estrategia')['Id_donante'].nunique()
    
    tabla_estrategia = pd.DataFrame({
        'Total Donantes': estrategia_donantes,
        'Monto Total': estrategia_monto['sum'].apply(lambda x: f"${x:,.0f}"),
        'Donación Promedio': estrategia_monto['mean'].apply(lambda x: f"${x:,.0f}"),
        'Total Fugados': estrategia_fugados.reindex(estrategia_donantes.index, fill_value=0).astype(int),
        'Tasa Fuga (%)': (estrategia_fugados.reindex(estrategia_donantes.index, fill_value=0) / estrategia_donantes * 100).round(2)
    })
    
    st.dataframe(tabla_estrategia, use_container_width=True)
    
    st.markdown("---")
    
    # Tabla Método de Pago (SEGUNDA)
    st.subheader("💳 Comparación por Método de Pago")
    
    metodo_donantes = df.groupby('Método_Pago')['Id_donante'].nunique()
    metodo_monto = df.groupby('Método_Pago')['Monto_Donacion'].agg(['sum', 'mean'])
    metodo_fugados = df_fugados.groupby('Método_Pago')['Id_donante'].nunique()
    
    tabla_metodo = pd.DataFrame({
        'Total Donantes': metodo_donantes,
        'Monto Total': metodo_monto['sum'].apply(lambda x: f"${x:,.0f}"),
        'Donación Promedio': metodo_monto['mean'].apply(lambda x: f"${x:,.0f}"),
        'Total Fugados': metodo_fugados.reindex(metodo_donantes.index, fill_value=0).astype(int),
        'Tasa Fuga (%)': (metodo_fugados.reindex(metodo_donantes.index, fill_value=0) / metodo_donantes * 100).round(2)
    })
    
    st.dataframe(tabla_metodo, use_container_width=True)

# ===============================
# TAB 4: LIFETIME ANALYSIS
# ===============================
with tab4:
    st.header("Análisis de Lifetime")
    
    if len(df_fugados) > 0:
        df_fugados_copy = df_fugados.copy()
        df_fugados_copy['Meses_Activo'] = (
            (pd.to_datetime(df_fugados_copy['Fecha_Fuga']) - 
             pd.to_datetime(df_fugados_copy['Fecha_Creacion'])).dt.days / 30
        )
        
        lifetime_promedio = df_fugados_copy.groupby('Año_Mes_Creacion')['Meses_Activo'].mean()
        
        col_lt1, col_lt2 = st.columns(2)
        
        with col_lt1:
            st.subheader("⏱️ Meses Activo Promedio por Cohorte")
            fig6, ax6 = plt.subplots(figsize=(10, 6))
            ax6.bar(range(len(lifetime_promedio)), lifetime_promedio.values, 
                   color='#A23B72', alpha=0.8)
            ax6.set_xticks(range(len(lifetime_promedio)))
            ax6.set_xticklabels(lifetime_promedio.index, rotation=45, ha='right')
            ax6.set_xlabel('Cohorte', fontsize=12)
            ax6.set_ylabel('Meses Promedio', fontsize=12)
            ax6.set_title('Tiempo Promedio Hasta la Fuga', fontsize=14)
            ax6.grid(axis='y', alpha=0.3)
            plt.tight_layout()
            st.pyplot(fig6)
        
        with col_lt2:
            st.subheader("📈 Estadísticas de Lifetime")
            st.metric("Lifetime Promedio Global", f"{lifetime_promedio.mean():.1f} meses")
            st.metric("Lifetime Máximo", f"{lifetime_promedio.max():.1f} meses")
            st.metric("Lifetime Mínimo", f"{lifetime_promedio.min():.1f} meses")
    else:
        st.info("No hay suficientes datos de donantes fugados para este análisis")