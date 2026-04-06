import streamlit as st
import pandas as pd
import geopandas as gpd
import folium
from streamlit_folium import folium_static
import os
from datetime import datetime

# ==============================================================================
# 1. CONFIGURACIÓN DEL CUARTO DE GUERRA (UI/UX)
# ==============================================================================
st.set_page_config(page_title="SIT-EDOMEX | Estrategia Toluca", page_icon="🦅", layout="wide")

st.markdown("""
    <style>
    .stMetric { background-color: #ffffff; padding: 15px; border-radius: 8px; border-left: 5px solid #FF8200; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }
    [data-testid="stMetricValue"] { font-size: 28px; color: #1E1E1E; font-weight: 800; }
    [data-testid="stMetricDelta"] svg { fill: #FF8200 !important; }
    .stTabs [data-baseweb="tab-list"] { gap: 8px; }
    .stTabs [data-baseweb="tab"] { background-color: #f0f2f6; border-radius: 4px; padding: 10px 16px; font-weight: 600; }
    .stTabs [aria-selected="true"] { background-color: #FF8200 !important; color: white !important; }
    </style>
    """, unsafe_allow_html=True)

if 'base_apoyos' not in st.session_state:
    st.session_state.base_apoyos = pd.DataFrame(columns=["Fecha", "Ciudadano", "Sección", "Tipo_Apoyo", "Estatus"])

# ==============================================================================
# 2. MOTOR DE CARGA BLINDADO
# ==============================================================================
@st.cache_data
def load_data():
    APP_DIR = os.path.dirname(os.path.abspath(__file__))
    ROOT_DIR = os.path.dirname(APP_DIR)
    ruta_geojson = os.path.join(APP_DIR, 'assets', 'MAPA_ESTRATEGICO_TOLUCA.geojson')
    ruta_maestro = os.path.join(ROOT_DIR, 'data', '03_processed', 'CRUCE_MAESTRO_INDICADORES.csv')

    if not os.path.exists(ruta_geojson) or not os.path.exists(ruta_maestro): 
        return None, None

    mapa = gpd.read_file(ruta_geojson)
    df_m = pd.read_csv(ruta_maestro)
    
    mapa['SECCION'] = mapa['SECCION'].astype(str).str.replace(r'\.0$', '', regex=True).str.strip()
    df_m['SECCION'] = df_m['SECCION'].astype(str).str.replace(r'\.0$', '', regex=True).str.strip()
    
    # Asegurar columnas críticas para el visualizador
    for col in ['BASELINE_MC', 'MUJERES_2026', 'HOMBRES_2026', 'ELECTORES_MENORES_25', 'ABSTENCION_PROYECTADA_2026']:
        if col not in df_m.columns: df_m[col] = 0
        
    return mapa, df_m

mapa_gdf, df_maestro = load_data()

if mapa_gdf is None:
    st.error("🚨 Ejecuta primero el Backend (cruce_maestro.py y cruce_espacial.py)")
    st.stop()

# ==============================================================================
# 3. SIDEBAR: SIMULADOR Y VARIABLES DE ENTORNO
# ==============================================================================
st.sidebar.image("https://upload.wikimedia.org/wikipedia/commons/3/3d/Movimiento_Ciudadano_Logo.svg", width=100)
st.sidebar.markdown("### 🎮 Simulador de Operación")
st.sidebar.caption("Define el alcance de la maquinaria.")

s_abs = st.sidebar.slider("Rescate Abstencionistas (%)", 0, 50, 15)
s_juv = st.sidebar.slider("Captación Jóvenes (%)", 0, 100, 30)
s_waze = st.sidebar.slider("Impacto Causa Ciudadana (%)", 0, 100, 20)

# ECUACIÓN SITS GLOBAL
df_maestro['PROYECCION_VOTOS'] = (
    df_maestro['BASELINE_MC'] + 
    (df_maestro['ABSTENCION_PROYECTADA_2026'] * (s_abs / 100)) +
    (df_maestro['ELECTORES_MENORES_25'] * (s_juv / 100)) +
    (df_maestro.get('TOTAL_QUEJAS_ACTIVAS', 0) * (s_waze / 10))
).round(0)

votos_actuales = int(df_maestro['PROYECCION_VOTOS'].sum())
META_GLOBAL = 80000

# ==============================================================================
# 4. DASHBOARD SUPERIOR (C5)
# ==============================================================================
st.title("🦅 SIT-EDOMEX | Comando Estratégico Toluca")
progreso = min(votos_actuales / META_GLOBAL, 1.0)
st.progress(progreso)
st.caption(f"🏁 **Termómetro de Victoria:** {progreso*100:.1f}% ({votos_actuales:,} de {META_GLOBAL:,} votos meta)")

k1, k2, k3, k4, k5 = st.columns(5)
k1.metric("Proyección SITS", f"{votos_actuales:,}", f"Faltan {META_GLOBAL - votos_actuales:,}" if votos_actuales < META_GLOBAL else "Meta Superada")
k2.metric("Padrón 2026", f"{int(df_maestro['LISTA_NOMINAL_2026'].sum()):,}")
k3.metric("Jóvenes < 25", f"{int(df_maestro['ELECTORES_MENORES_25'].sum()):,}")
k4.metric("Mujeres", f"{int(df_maestro['MUJERES_2026'].sum()):,}")
k5.metric("Abstencionistas (24)", f"{int(df_maestro['ABSTENCION_PROYECTADA_2026'].sum()):,}")

st.markdown("---")

t_mapa, t_diag, t_crm, t_data = st.tabs(["📍 1. Radar Cartográfico", "🎯 2. Plan de Vuelo Seccional", "👥 3. Red de Promoción", "🗄️ 4. Base de Datos"])

# --- TAB 1: RADAR CARTOGRÁFICO MULTICAPA ---
with t_mapa:
    c_filtros, c_mapa = st.columns([1, 4])
    
    with c_filtros:
        st.markdown("### 👁️ Óptica del Mapa")
        capa_seleccionada = st.radio("Selecciona la capa de análisis:", [
            "🔥 Prioridad SITS (Global)",
            "👩 Densidad Femenina",
            "🧑‍🎓 Concentración de Jóvenes",
            "🛑 Zonas de Abstencionismo",
            "🧡 Voto MC Histórico (2024)"
        ])
        
        # Diccionario de capas: (Columna_DF, Paleta_de_Color, Alias_Tooltip)
        dic_capas = {
            "🔥 Prioridad SITS (Global)": ("INDICE_PRIORIDAD_SITS", "YlOrRd", "Prioridad:"),
            "👩 Densidad Femenina": ("MUJERES_2026", "PuRd", "Total Mujeres:"),
            "🧑‍🎓 Concentración de Jóvenes": ("ELECTORES_MENORES_25", "BuPu", "Total Jóvenes:"),
            "🛑 Zonas de Abstencionismo": ("ABSTENCION_PROYECTADA_2026", "Oranges", "Bolsa Abstención:"),
            "🧡 Voto MC Histórico (2024)": ("BASELINE_MC", "Oranges", "Votos MC 2024:")
        }
        col_activa, paleta, alias = dic_capas[capa_seleccionada]
        
        st.info("💡 **Tip de Operación:** Usa la capa de 'Mujeres' para enviar brigadas con propuestas de seguridad y salud familiar. Usa la de 'Jóvenes' para activaciones digitales.")

    with c_mapa:
        mapa_vis = mapa_gdf[['SECCION', 'geometry']].merge(df_maestro, on='SECCION', how='left')
        m = folium.Map(location=[19.29, -99.65], zoom_start=12, tiles="cartodbpositron")
        
        folium.Choropleth(
            geo_data=mapa_vis, name=capa_seleccionada, data=mapa_vis,
            columns=["SECCION", col_activa], key_on="feature.properties.SECCION",
            fill_color=paleta, fill_opacity=0.75, line_opacity=0.3, legend_name=capa_seleccionada
        ).add_to(m)
        
        folium.GeoJson(
            mapa_vis, style_function=lambda x: {'fillColor': 'transparent', 'color': 'transparent'},
            tooltip=folium.GeoJsonTooltip(
                fields=['SECCION', col_activa, 'PROYECCION_VOTOS'],
                aliases=['📌 Sección:', f'📊 {alias}', '🎯 Meta de Votos:'],
                localize=True
            )
        ).add_to(m)
        folium_static(m, width=1000)

# --- TAB 2: DIAGNÓSTICO PROFUNDO (PLAN DE VUELO) ---
with t_diag:
    st.markdown("### 📋 Expediente Táctico por Sección")
    
    # ORDENAMIENTO INTELIGENTE: Las más prioritarias aparecen primero
    secciones_ordenadas = df_maestro.sort_values(by='INDICE_PRIORIDAD_SITS', ascending=False)['SECCION'].tolist()
    
    sel_sec = st.selectbox("Selecciona la sección a analizar (Ordenadas de mayor a menor prioridad):", secciones_ordenadas)
    sec_data = df_maestro[df_maestro['SECCION'] == sel_sec].iloc[0]
    
    # Cálculos Específicos
    votos_24 = sec_data.get('BASELINE_MC', 0)
    meta_sec = sec_data['PROYECCION_VOTOS']
    crecimiento_abs = meta_sec - votos_24
    pct_crecimiento = (crecimiento_abs / votos_24 * 100) if votos_24 > 0 else (100 if meta_sec > 0 else 0)
    aporte_meta = (meta_sec / META_GLOBAL) * 100
    
    st.markdown(f"#### Resumen de Misión: Sección **{sel_sec}**")
    
    m1, m2, m3, m4 = st.columns(4)
    m1.metric("🟠 Resultado Histórico 2024", int(votos_24), "Votos base consolidados", delta_color="off")
    m2.metric("🎯 Meta Proyectada 2026", int(meta_sec), f"+{int(crecimiento_abs)} votos requeridos")
    m3.metric("🚀 Esfuerzo de Crecimiento", f"{pct_crecimiento:.1f}%", "Incremento sobre el piso base")
    m4.metric("📈 Aporte a la Gran Meta", f"{aporte_meta:.3f}%", f"De los {META_GLOBAL:,} totales")
    
    st.markdown("---")
    col_izq, col_der = st.columns(2)
    
    with col_izq:
        st.markdown("#### 👤 Composición Demográfica (INE 2026)")
        st.write(f"- **Padrón Total:** {int(sec_data['LISTA_NOMINAL_2026']):,}")
        st.write(f"- **Mujeres:** {int(sec_data['MUJERES_2026']):,} ({(sec_data['MUJERES_2026']/sec_data['LISTA_NOMINAL_2026']*100 if sec_data['LISTA_NOMINAL_2026']>0 else 0):.1f}%)")
        st.write(f"- **Hombres:** {int(sec_data['HOMBRES_2026']):,} ({(sec_data['HOMBRES_2026']/sec_data['LISTA_NOMINAL_2026']*100 if sec_data['LISTA_NOMINAL_2026']>0 else 0):.1f}%)")
        st.write(f"- **Jóvenes (<25):** {int(sec_data['ELECTORES_MENORES_25']):,}")
        st.write(f"- **Bolsa de Abstención:** {int(sec_data['ABSTENCION_PROYECTADA_2026']):,} (Mercado recuperable)")

    with col_der:
        st.markdown("#### 🧠 Dictamen Estratégico (IA)")
        if sec_data['INDICE_PRIORIDAD_SITS'] > 75:
            st.error("🔥 **ZONA CALIENTE (Prioridad Máxima):** El candidato DEBE visitar esta sección. Alta concentración de votantes volátiles y gran bolsa de abstencionismo.")
        elif sec_data['INDICE_PRIORIDAD_SITS'] > 40:
            st.warning("⚠️ **ZONA TÁCTICA:** Enviar avanzada y brigadas. Visita del candidato recomendada si la agenda lo permite.")
        else:
            st.success("🟢 **ZONA BASE:** Operar mediante estructura digital y de promoción. No gastar tiempo primario del candidato aquí.")
            
        if sec_data.get('TOTAL_QUEJAS_ACTIVAS', 0) > 0:
            st.info(f"🚧 **Enojo Urbano:** {int(sec_data['TOTAL_QUEJAS_ACTIVAS'])} quejas de baches/servicios. El discurso debe ser de obra pública y gestión municipal.")

    with st.expander("Ver todas las variables matemáticas de esta sección"):
        st.dataframe(sec_data.to_frame(name="Valor SITS"))

# --- TAB 3: RED DE PROMOCIÓN (CRM) ---
with t_crm:
    st.markdown("### 📇 Red de Promotores y Simpatizantes")
    c_form, c_tabla = st.columns([1, 2])
    with c_form:
        with st.form("crm_form", clear_on_submit=True):
            c_nom = st.text_input("Nombre Completo (Líder / Simpatizante)")
            c_sec = st.selectbox("Sección de Influencia", df_maestro['SECCION'].unique()) # Aquí sí alfabetico es más rápido para buscar
            c_tipo = st.selectbox("Nivel de Compromiso", ["RC (Representante)", "Promotor de Cuadra", "Gestión (Petición)", "Simpatizante"])
            if st.form_submit_button("Registrar Elemento"):
                new = pd.DataFrame([{"Fecha": datetime.now().strftime("%Y-%m-%d"), "Nombre": c_nom, "Sección": c_sec, "Rol": c_tipo, "Estatus": "Activo"}])
                st.session_state.base_apoyos = pd.concat([st.session_state.base_apoyos, new], ignore_index=True)
                st.success("Operador añadido a la red.")
    with c_tabla:
        st.dataframe(st.session_state.base_apoyos, use_container_width=True)

# --- TAB 4: BASE DE DATOS MAESTRA ---
with t_data:
    st.markdown("### 🗄️ Extracción de Inteligencia en Crudo")
    st.dataframe(df_maestro, use_container_width=True)
    st.download_button("📥 Exportar Modelo a Excel (.csv)", df_maestro.to_csv(index=False).encode('utf-8'), "SITS_ESTRATEGIA_TOLUCA.csv", "text/csv")