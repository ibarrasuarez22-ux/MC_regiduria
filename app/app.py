import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import os
import json

# ==============================================================================
# CONFIGURACIÓN DE PÁGINA (ESTILO CONSULTORÍA PREMIUM)
# ==============================================================================
st.set_page_config(
    page_title="SIT Toluca | Centro de Comando", 
    layout="wide", 
    initial_sidebar_state="expanded"
)

# Universo de Influencia del Regidor (165 Secciones)
SECCIONALES_REGIDOR = (
    '5426', '5428', '5429', '5430', '5431', '5432', '5433', '5437', '5438', '5419', '5420', '5421', '5422', '5423', '5424', '5425', '5427', '5434', '5435', '5436',
    '5233', '5242', '5259', '5260', '5261', '5342', '5343', '5344', '5345', '5347', '5348', '5349', '5350', '5351', '5352', '5353', '5223', '5277', '5278', '5279',
    '5282', '5283', '5284', '5290', '5291', '5292', '5300', '5301', '5177', '5178', '5179', '5199', '5200', '5201', '5202', '5203', '5204', '5205', '5198', '5206',
    '5207', '5208', '5209', '5307', '5308', '5313', '5314', '5315', '5217', '5237', '5254', '5255', '5256', '5257', '5258', '5337', '5338', '5339', '5340', '5341',
    '5215', '5216', '5329', '5330', '5331', '5332', '5333', '5334', '5335', '5336', '5238', '5239', '5240', '5241', '5316', '5317', '5318', '5319', '5320', '5262',
    '5280', '5281', '5289', '5295', '5296', '5297', '5414', '5415', '5416', '5417', '5418', '5293', '5294', '5299', '5309', '5310', '5311', '5312', '5321', '5322',
    '5324', '5325', '5326', '5328', '5194', '5195', '5210', '5211', '5212', '5213', '5221', '5222', '5224', '5225', '5232', '5196', '5197', '5214', '5218', '5219',
    '5220', '5234', '5235', '5236', '5226', '5227', '5228', '5229', '5230', '5231', '5243', '5244', '5245', '5246', '5247', '5248', '5249', '5250', '5251', '5252',
    '5253', '5263', '5264', '5265', '5327'
)

# ==============================================================================
# CARGA BLINDADA DE DATOS
# ==============================================================================
@st.cache_data
def cargar_datos():
    APP_DIR = os.path.dirname(os.path.abspath(__file__))
    BASE_DIR = os.path.dirname(APP_DIR) 
    
    ruta_datos = os.path.join(BASE_DIR, 'data', '03_processed', 'CRUCE_MAESTRO_INDICADORES.csv')
    ruta_mapa = os.path.join(BASE_DIR, 'data', '03_processed', 'MAPA_TOLUCA_SITS.geojson')
    
    try:
        df = pd.read_csv(ruta_datos)
        # Limpieza forense de columnas y casillas
        df.columns = df.columns.astype(str).str.strip().str.upper()
        df['SECCION'] = df['SECCION'].astype(str).str.strip().str.replace('.0', '', regex=False)
    except FileNotFoundError:
        return pd.DataFrame(), None

    geojson_data = None
    if os.path.exists(ruta_mapa):
        with open(ruta_mapa, 'r', encoding='utf-8') as f:
            geojson_data = json.load(f)
            # Enlace estricto del mapa con la tabla
            for feature in geojson_data['features']:
                sec = str(feature['properties'].get('SECCION', '')).strip().replace('.0', '')
                feature['properties']['SECCION'] = sec
                feature['id'] = sec 
            
    return df, geojson_data

def convertir_df_a_csv(df):
    return df.to_csv(index=False).encode('utf-8')

# Inicialización
df_maestro, geojson_data = cargar_datos()

if df_maestro is None or df_maestro.empty:
    st.error("⚠️ La base de datos está vacía. Verifica el archivo en 03_processed.")
    st.stop()

# ==============================================================================
# BARRA LATERAL: MICROTARGETING Y ESTRATEGIA
# ==============================================================================
st.sidebar.image("https://upload.wikimedia.org/wikipedia/commons/thumb/1/1a/Movimiento_Ciudadano_%28Mexico%29_logo.svg/1200px-Movimiento_Ciudadano_%28Mexico%29_logo.svg.png", width=120)
st.sidebar.title("SIT Toluca")
st.sidebar.markdown("**Sistema de Inteligencia Territorial**")
st.sidebar.markdown("---")

st.sidebar.markdown("### 🎛️ Filtros de Microtargeting")
st.sidebar.caption("Corta el municipio a tu medida. Usa estos controles para aislar solo las zonas rentables.")

# Controles blindados contra nulos
max_jovenes = int(df_maestro['ELECTORES_MENORES_25'].max()) if 'ELECTORES_MENORES_25' in df_maestro.columns else 100

filtro_jovenes = st.sidebar.slider("👦 Mínimo de Jóvenes por Sección:", min_value=0, max_value=max_jovenes, value=0, help="Filtra calles universitarias o de nuevos votantes.")
filtro_irc = st.sidebar.slider("🧱 Tope de Control Rival (IRC) %:", min_value=0, max_value=100, value=100, help="Bájalo para ocultar las zonas secuestradas por operadores del PRI o Morena.")
filtro_iee = st.sidebar.slider("🔥 Mínimo de Volatilidad (IEE) %:", min_value=0, max_value=100, value=0, help="Súbelo para ver solo las calles donde la gente cambia su voto fácilmente.")

df_filtrado_global = df_maestro[
    (df_maestro.get('ELECTORES_MENORES_25', 0) >= filtro_jovenes) &
    (df_maestro.get('INDICE_IRC', 0) <= filtro_irc) &
    (df_maestro.get('INDICE_IEE', 0) >= filtro_iee)
]

# ==============================================================================
# MOTOR VISUAL REUTILIZABLE (SE USA PARA REGIDOR Y GLOBAL)
# ==============================================================================
def generar_dashboard(df_visual, geojson, prefijo):
    
    # --- BLOQUE 1: KPIs RESUMEN EJECUTIVO ---
    st.markdown("### 📈 Panorama de la Zona Seleccionada")
    k1, k2, k3, k4 = st.columns(4)
    
    k1.metric("Secciones Activas", f"{len(df_visual)}", help="Total de secciones que pasaron tus filtros.")
    
    meta = df_visual['VOTOS_TOTALES_PROYECTADOS_2026'].sum() if 'VOTOS_TOTALES_PROYECTADOS_2026' in df_visual.columns else 0
    k2.metric("Meta SITS (Votos Proyectados)", f"{int(meta):,}", help="La cantidad de votos realista que podemos obtener si operamos al 100% esta zona.")
    
    orfandad = df_visual['ORFANDAD_NARANJA'].sum() if 'ORFANDAD_NARANJA' in df_visual.columns else 0
    k3.metric("Orfandad Naranja (Votos flotantes)", f"{int(orfandad):,}", help="Votantes que ya cruzaron el logo de MC en el pasado. Es nuestro piso recuperable.")
    
    abstencion = df_visual['ABSTENCION_PROYECTADA_2026'].sum() if 'ABSTENCION_PROYECTADA_2026' in df_visual.columns else 0
    k4.metric("Abstencionismo Fijo", f"{int(abstencion):,}", help="Población que nunca sale a votar. No gastemos recursos publicitarios en ellos.")
    
    st.markdown("---")

    # --- SUB-PESTAÑAS ORGANIZADORAS ---
    s_mapa, s_analitica, s_ficha, s_presupuesto = st.tabs([
        "🗺️ 1. Mapa de Calor", 
        "📊 2. Analítica de Voto", 
        "🔍 3. Rayos X por Calle", 
        "💰 4. Inversión y ROI"
    ])

    # 🗺️ 1. MAPA DE CALOR CARTOGRÁFICO
    with s_mapa:
        st.markdown("#### Radar Territorial de Toluca")
        capa = st.selectbox("¿Qué indicador deseas visualizar geográficamente?", [
            "A) Dónde hay votos sueltos (Orfandad Naranja)",
            "B) Dónde la gente cambia de partido (Volatilidad IEE)",
            "C) Dónde hay control enemigo (Clientelismo IRC)",
            "D) Dónde están los nuevos votantes (Jóvenes Sub-25)"
        ], key=f"capa_mapa_{prefijo}")
        
        # Asignación de variables según selección
        if "Orfandad" in capa:
            col_z, esc_z, lbl_z = 'ORFANDAD_NARANJA', 'YlOrBr', 'Votos Sueltos'
        elif "Volatilidad" in capa:
            col_z, esc_z, lbl_z = 'INDICE_IEE', 'Oranges', 'Volatilidad %'
        elif "Clientelismo" in capa:
            col_z, esc_z, lbl_z = 'INDICE_IRC', 'Reds', 'Control Enemigo %'
        else:
            col_z, esc_z, lbl_z = 'ELECTORES_MENORES_25', 'Blues', 'Jóvenes'

        # Render del Mapa Plotly (Estilo NASA Oscuro)
        if geojson is not None:
            fig_mapa = px.choropleth_mapbox(
                df_visual,
                geojson=geojson,
                locations='SECCION',
                featureidkey='properties.SECCION',
                color=col_z,
                color_continuous_scale=esc_z,
                mapbox_style="carto-darkmatter",
                zoom=11,
                center={"lat": 19.2826, "lon": -99.6557},
                opacity=0.8,
                hover_name="SECCION",
                hover_data={"SECCION": False, col_z: True, "VOTOS_TOTALES_PROYECTADOS_2026": True},
                labels={col_z: lbl_z, "VOTOS_TOTALES_PROYECTADOS_2026": "Votos Meta"}
            )
            fig_mapa.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
            st.plotly_chart(fig_mapa, use_container_width=True)
            
            # 📖 EXPLICACIÓN PEDAGÓGICA (ACORDEÓN)
            with st.expander("📖 ¿Cómo interpretar este mapa para la campaña?"):
                st.markdown("""
                **Guía de Uso Estratégico:**
                * Pasa el ratón (o el dedo) sobre cualquier polígono para ver el número de sección y cuántos votos nos debe dar (Votos Meta).
                * **Las zonas más intensas (brillantes):** Son los "Hotspots". Si estás viendo 'Volatilidad', manda a los candidatos a caminar ahí; la gente los va a escuchar. Si estás viendo 'Clientelismo', aléjate, te van a reventar los eventos o nadie va a salir de sus casas.
                * Usa este mapa para trazar las rutas de las avanzadas y el volanteo de los brigadistas.
                """)
        else:
            st.warning("No hay capa cartográfica disponible.")

    # 📊 2. ANALÍTICA DE VOTO (GRÁFICAS DE EXPLICACIÓN)
    with s_analitica:
        st.markdown("#### Inteligencia Descriptiva de Secciones")
        
        c_graf1, c_graf2 = st.columns(2)
        with c_graf1:
            st.markdown("##### 🏆 Ranking de Casillas (Las Joyas de la Corona)")
            top_15 = df_visual.nlargest(15, 'VOTOS_TOTALES_PROYECTADOS_2026')
            fig_bar = px.bar(
                top_15, x='VOTOS_TOTALES_PROYECTADOS_2026', y='SECCION', orientation='h',
                color='INDICE_IEE', color_continuous_scale='Oranges', text_auto='.0f',
                labels={'VOTOS_TOTALES_PROYECTADOS_2026': 'Votos a Ganar', 'SECCION': 'Sección', 'INDICE_IEE': 'Apertura al cambio %'}
            )
            fig_bar.update_layout(yaxis={'categoryorder':'total ascending'})
            st.plotly_chart(fig_bar, use_container_width=True)
            
            with st.expander("📖 ¿Qué me dice esta gráfica?"):
                st.markdown("Te ordena de mayor a menor las 15 calles que más votos le pueden dar a MC. **El color importa:** Entre más anaranjada sea la barra, más fácil será convencer a esos vecinos; si la barra es oscura, nos darán votos, pero costará más sudor y saliva convencerlos.")

        with c_graf2:
            st.markdown("##### 🎯 Cuadrantes de Operación (Volatilidad vs Control)")
            if 'INDICE_IRC' in df_visual.columns and 'INDICE_IEE' in df_visual.columns:
                fig_scat = px.scatter(
                    df_visual, x='INDICE_IRC', y='INDICE_IEE', size='VOTOS_TOTALES_PROYECTADOS_2026',
                    color='ORFANDAD_NARANJA', hover_name='SECCION', color_continuous_scale='Purples',
                    labels={'INDICE_IRC': 'Clientelismo Rival (IRC %)', 'INDICE_IEE': 'Volatilidad (IEE %)', 'ORFANDAD_NARANJA': 'Votos Sueltos MC'}
                )
                media_iee_loc = df_visual['INDICE_IEE'].mean()
                media_irc_loc = df_visual['INDICE_IRC'].mean()
                fig_scat.add_hline(y=media_iee_loc, line_dash="dash", line_color="gray", annotation_text="Promedio Cambio")
                fig_scat.add_vline(x=media_irc_loc, line_dash="dash", line_color="gray", annotation_text="Promedio Control")
                st.plotly_chart(fig_scat, use_container_width=True)
                
                with st.expander("📖 ¿Cómo elegir dónde pelear usando esta gráfica?"):
                    st.markdown("""
                    **Esta es la gráfica más importante de la campaña:**
                    * **Arriba a la Izquierda:** El premio mayor. La gente cambia su voto y los caciques rivales no tienen fuerza. Ataca aquí con todo.
                    * **Arriba a la Derecha:** La zona de guerra. La gente quiere cambiar, pero las maquinarias rivales están operando duro. Hay que meter choque.
                    * **Abajo a la Derecha:** Zona perdida. El PRI o Morena controlan a la gente y nadie cambia de opinión. **No entres aquí.**
                    """)

    # 🔍 3. RAYOS X POR CALLE (FICHA TÁCTICA)
    with s_ficha:
        st.markdown("#### 🔍 Auditoría Focalizada de Casilla")
        st.caption("Escribe la sección específica que el Regidor va a visitar mañana para entregarle su ficha técnica.")
        
        sec_elegida = st.selectbox("Selecciona la Sección Electoral:", df_visual['SECCION'].sort_values(), key=f"sel_sec_{prefijo}")
        
        if sec_elegida:
            row_sec = df_visual[df_visual['SECCION'] == sec_elegida].head(1).squeeze()
            
            col_rx1, col_rx2 = st.columns((3, 1))
            with col_rx1:
                st.markdown(f"**📍 Expediente de Operación: Sección {sec_elegida}**")
            with col_rx2:
                # 💥 CURA: AÑADIDO KEY ÚNICO AL BOTÓN DE DESCARGA
                st.download_button(
                    "📥 Imprimir Ficha (CSV)", 
                    data=convertir_df_a_csv(df_visual[df_visual['SECCION'] == sec_elegida]), 
                    file_name=f"Ficha_Sec_{sec_elegida}.csv", 
                    mime="text/csv",
                    key=f"dl_ficha_{prefijo}_{sec_elegida}"
                )
            
            # Velocímetros (Gauges)
            g1, g2, g3 = st.columns(3)
            with g1:
                val_iee = float(row_sec.get('INDICE_IEE', 0))
                fig_g1 = go.Figure(go.Indicator(
                    mode = "gauge+number", value = val_iee, title = {'text': "Probabilidad de Persuasión %"},
                    gauge = {'axis': {'range': (0, 100)}, 'bar': {'color': "orange"}}
                ))
                fig_g1.update_layout(height=200, margin={'t':40, 'b':0, 'l':0, 'r':0})
                st.plotly_chart(fig_g1, use_container_width=True)
            with g2:
                val_irc = float(row_sec.get('INDICE_IRC', 0))
                fig_g2 = go.Figure(go.Indicator(
                    mode = "gauge+number", value = val_irc, title = {'text': "Riesgo de Control Enemigo %"},
                    gauge = {'axis': {'range': (0, 100)}, 'bar': {'color': "red"}}
                ))
                fig_g2.update_layout(height=200, margin={'t':40, 'b':0, 'l':0, 'r':0})
                st.plotly_chart(fig_g2, use_container_width=True)
            with g3:
                val_abs = float(row_sec.get('ABSTENCION_PROYECTADA_2026', 0))
                val_ln = max(1.0, float(row_sec.get('LISTA_NOMINAL_2026', 1)))
                part_pct = 100.0 - ((val_abs / val_ln) * 100.0)
                fig_g3 = go.Figure(go.Indicator(
                    mode = "gauge+number", value = part_pct, title = {'text': "Tasa de Participación Esperada %"},
                    gauge = {'axis': {'range': (0, 100)}, 'bar': {'color': "green"}}
                ))
                fig_g3.update_layout(height=200, margin={'t':40, 'b':0, 'l':0, 'r':0})
                st.plotly_chart(fig_g3, use_container_width=True)

            # Cifras duras
            st.markdown("##### 🧾 Composición Demográfica de la Cuadra")
            md1, md2, md3, md4 = st.columns(4)
            md1.metric("Padrón Total (Gente)", f"{int(val_ln):,}")
            md2.metric("Gente que no Vota", f"{int(val_abs):,}")
            md3.metric("Votos de MC Sueltos", f"{int(row_sec.get('ORFANDAD_NARANJA', 0)):,}")
            md4.metric("Nuestra Meta 2026", f"{int(row_sec.get('VOTOS_TOTALES_PROYECTADOS_2026', 0)):,}")
            
            with st.expander("📖 ¿Qué le digo al Brigadista sobre esta sección? (Dictamen AI)"):
                media_iee_loc = df_visual['INDICE_IEE'].mean() if 'INDICE_IEE' in df_visual.columns else 0
                if val_iee > media_iee_loc:
                    st.success("🗣️ **Dile a tu equipo:** 'Esta zona está enojada con el gobierno actual. Toquen puertas, escuchen sus quejas y presenten la alternativa nueva. La gente aquí SÍ cambia su voto si los convencemos.'")
                else:
                    st.error("🗣️ **Dile a tu equipo:** 'Esta zona está fuertemente amarrada a los apoyos sociales de otros partidos. El discurso bonito no sirve aquí. Tienen que buscar a los líderes de manzana y ofrecer soluciones reales a problemas de la calle (agua, luz).'")

    # 💰 4. PRESUPUESTO Y ROI
    with s_presupuesto:
        st.markdown("### 💰 Calculadora de Operación de Campo (Costeo)")
        st.info("Ajusta las tarifas. El sistema te dirá cuánto dinero cuesta operar **únicamente las secciones que pasaron tus filtros** de la barra lateral izquierda.")
        
        col_c1, col_c2, col_c3 = st.columns(3)
        costo_v = col_c1.number_input("Costo por Lona/Utilitario/Volante ($)", value=2.00, step=0.50, key=f"c_vol_{prefijo}")
        costo_b = col_c2.number_input("Pago diario a Movilizador/Brigada ($)", value=350.0, step=50.0, key=f"c_bri_{prefijo}")
        costo_d = col_c3.number_input("Inversión Digital (Pauta) por Joven ($)", value=10.0, step=1.0, key=f"c_dig_{prefijo}")

        dias = st.slider("Días que la brigada estará caminando la zona:", 1, 60, 30, key=f"sl_dias_{prefijo}")
        
        tot_ln = float(df_visual['LISTA_NOMINAL_2026'].sum()) if 'LISTA_NOMINAL_2026' in df_visual.columns else 0
        tot_jv = float(df_visual['ELECTORES_MENORES_25'].sum()) if 'ELECTORES_MENORES_25' in df_visual.columns else 0
        tot_meta = float(df_visual['VOTOS_TOTALES_PROYECTADOS_2026'].sum()) if 'VOTOS_TOTALES_PROYECTADOS_2026' in df_visual.columns else 0
        
        brigs = max(1, int(tot_ln / 500)) # Fórmula SITS: 1 persona cubre 500 listados
        
        gasto_tierra = brigs * costo_b * dias
        gasto_aire = tot_ln * costo_v
        gasto_digital = tot_jv * costo_d
        
        total_roi = gasto_tierra + gasto_aire + gasto_digital
        cac = total_roi / tot_meta if tot_meta > 0 else 0
        
        st.markdown("---")
        r1, r2, r3 = st.columns(3)
        r1.metric("Caja Requerida (Inversión Total)", f"${total_roi:,.2f} MXN")
        r2.metric("Nómina Requerida (1 x 500 habs)", f"{brigs} Personas")
        r3.metric("Costo Promedio por Voto (CAC)", f"${cac:,.2f} / voto")

# ==============================================================================
# ESTRUCTURA PRINCIPAL (LAS DOS GRANDES PESTAÑAS)
# ==============================================================================
st.title("🎦 Centro de Comando y Gobernanza")
st.caption("Visión directiva y despliegue territorial.")

# Regresamos a las dos grandes pestañas originales
tab_macro, tab_regidor = st.tabs([
    "🌍 VISIÓN TOLUCA (Escenario Municipal Completo)", 
    "🎯 OPERACIÓN REGIDOR (Las 165 Secciones Clave)"
])

with tab_macro:
    if df_filtrado_global.empty:
        st.warning("Los filtros de la izquierda son demasiado estrictos. No hay secciones en Toluca con esas características.")
    else:
        generar_dashboard(df_filtrado_global, geojson_data, "macro")

with tab_regidor:
    df_filtrado_regidor = df_filtrado_global[df_filtrado_global['SECCION'].isin(SECCIONALES_REGIDOR)]
    if df_filtrado_regidor.empty:
        st.warning("Los filtros de la izquierda eliminaron todas las secciones de tu territorio de influencia (165). Relaja los filtros.")
    else:
        generar_dashboard(df_filtrado_regidor, geojson_data, "regidor")