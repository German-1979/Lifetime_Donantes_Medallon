# 🧩 Análisis de Lifetime y Cohortes de Donantes  
### *Pipeline ETL automatizado con Airflow + análisis avanzado en Streamlit*

Este proyecto modela el comportamiento real de los donantes de ONG’s, simulando aportes mensuales, fugas, pagos fallidos y patrones de retención.  
Incluye un pipeline completo ETL tipo Medallón (*Raw → Bronze → Silver → Gold*), orquestado con **Airflow** y visualizado mediante un **dashboard en Streamlit**.

---

## 🚀 Propósito del proyecto

Las ONG enfrentan preguntas claves:

- ¿Cuánto dura un donante antes de abandonar?
- ¿Qué cohortes generan más ingresos en el tiempo?
- ¿Qué canal (Face to Face vs Telemarketing) retiene mejor?
- ¿Qué métodos de pago fallan más?
- ¿Cómo evoluciona la base mes a mes?

Para responderlas, este proyecto:

1. **Genera datos sintéticos realistas**
2. **Procesa un pipeline ETL completo**
3. **Construye un análisis de cohortes**
4. **Calcula Lifetime Value (LTV), fuga, ingresos y retención**
5. **Entrega un dashboard interactivo**

---

## 🧱 Arquitectura del Proyecto

Proyecto: Lifetime_Donantes_Medallon

├── airflow/ # DAG diario de Airflow
├── layer/
│ ├── raw/ # Datos sintéticos
│ ├── bronze/ # Limpieza
│ ├── silver/ # Base para cohortes
│ ├── gold/ # KPIs finales
├── scripts/
│ ├── generacion_datos_sinteticos.py
│ ├── bronze_layer.py
│ ├── silver_layer.py
│ ├── gold_layer.py
│ └── streamlit_dashboard.py
├── main.py # Ejecuta pipeline local
├── docker-compose.yaml # Levanta Airflow
└── Dockerfile



---

## 🔬 1. Generación de Datos Sintéticos

Archivo: `scripts/generacion_datos_sinteticos.py`

Simula dinámicas reales de donantes en ONG:

- **1000 nuevos socios cada mes**
- **Tasa de fuga mensual: 2%**
- Período: **Jun 2023 – May 2025**
- Métodos de Pago:
  - Cuenta Corriente (97%)
  - Tarjeta Crédito (93%)
  - Cuenta Vista (85%)
  - Cuenta Rut (70%)
- Estrategias:
  - Face to Face (80%)
  - Telemarketing (20%)
- Simulación de:
  - Pagos exitosos y fallidos
  - Fugas tempranas sin haber donado
  - Fugas posteriores con historial
  - Montos variables
  - Lifecycle completo del donante

### Variables generadas

| Variable | Descripción |
|---------|-------------|
| Id_donante | ID único |
| Método_Pago | Medio de cobro |
| Estrategia | Canal de captación |
| Fecha_Creacion | Mes de ingreso |
| Fecha_Pago | Fecha del intento de cobro |
| Monto_Donacion | Monto aportado o 0 |
| Status_Socio | Activo o Fugado |
| Fecha_Fuga | Día de fuga |
| Año_Mes_Creacion | Cohorte |
| Año_Mes_Donacion | Período de aporte |
| Año_Mes_Fuga | Período de baja |

### Ejemplo de registros

| Id_donante | Método_Pago | Estrategia | Fecha_Creacion | Fecha_Pago | Monto_Donacion | Status_Socio | Fecha_Fuga |
|------------|-------------|------------|----------------|------------|----------------|--------------|------------|
| D000001 | Cuenta Rut | Face to Face | 2023-06-01 | 2023-06-10 | 0 | Activo | — |
| D000002 | Cuenta Corriente | Telemarketing | 2023-06-01 | 2023-06-15 | 9000 | Activo | — |
| D000003 | Tarjeta Crédito | Face to Face | 2023-06-01 | — | — | Fugado | 2023-06-08 |
| D000004 | Cuenta Vista | Face to Face | 2023-07-01 | 2023-07-11 | 10000 | Activo | — |

---

## 🏛️ 2. ETL tipo Medallón

### Raw  
Datos sintéticos generados automáticamente.

### Bronze  
Limpieza, estandarización de tipos, normalización.

### Silver  
Transformaciones clave para cohortes:  
- Pivot  
- Orden temporal  
- Filtros  
- Cálculos base LTV

### Gold  
KPIs para visualización:  
- Ingresos por cohorte  
- Cantidad de donantes  
- Monto total  
- Retención  
- Fuga  
- Lifetime

---

## 🎨 3. Dashboard Streamlit

Archivo: `scripts/streamlit_dashboard.py`

Incluye:

### 🔥 Heatmaps
- Retención porcentual por cohorte
- Donantes activos por período
- Ingresos por cohorte

### 📊 Métricas Clave
- Tasa de fuga global
- Donación promedio
- LTV promedio
- Tabla resumen por cohorte
- Gráficos de donantes y montos por cohorte

### 🎯 Segmentación
- Face to Face vs Telemarketing
- Método de pago
- Fuga por segmento
- Montos por segmento

### ⏱️ Lifetime
- Meses promedio activo antes de fuga
- Distribución por cohorte
- Mínimo / máximo / promedio

### ¿Qué se busca con un análisis de cohortes?

- Comparar rendimiento de cada generación de donantes
- Entender retención real vs fuga
- Identificar cohortes débiles
- Evaluar efectividad por canal
- Medir ingresos por mes desde la creación

En simple: **entender el ciclo de vida del donante**.

---

## 🛠️ 4. Clonar este proyecto

```bash
git clone https://github.com/tu_usuario/tu_repo.git
cd tu_repo


