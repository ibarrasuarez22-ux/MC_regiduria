import pandas as pd
import geopandas as gpd
import os
import warnings
import json
import numpy as np

warnings.filterwarnings('ignore')

# ==============================================================================
# CONFIGURACIÓN DE RUTAS ABSOLUTAS (BLINDAJE DE DIRECTORIOS)
# ==============================================================================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROCESSED_DIR = os.path.join(BASE_DIR, 'data', '03_processed')

# Rutas Cartográficas
RUTA_SHP_INEGI = os.path.join(BASE_DIR, 'data', '01_raw', 'cartografia_inegi', '15a.shp')
RUTA_SHP_INE = os.path.join(BASE_DIR, 'data', '01_raw', 'shapefiles', 'SECCION.shp')

# Rutas Tabulares
RUTA_CENSO_AGEB = os.path.join(BASE_DIR, 'data', '01_raw', 'censo_marginacion', 'RESAGEBURB_15CSV20.csv')
RUTA_MAESTRA_SITS = os.path.join(PROCESSED_DIR, 'CRUCE_MAESTRO_INDICADORES.csv')
RUTA_SALIDA_GEOJSON = os.path.join(PROCESSED_DIR, 'MAPA_TOLUCA_SITS.geojson')

# ==============================================================================
# FASE 1: PROCESAMIENTO DEL CENSO (BUSCADOR SEMÁNTICO ANTIFALLOS)
# ==============================================================================
def procesar_marginacion_ageb():
    print("▶ Extrayendo indicadores de pobreza extrema del Censo 2020 (INEGI)...")
    if not os.path.exists(RUTA_CENSO_AGEB):
        print(f"  ❌ Error: No se encontró el censo a nivel AGEB en {RUTA_CENSO_AGEB}")
        return pd.DataFrame()

    df_censo = pd.read_csv(RUTA_CENSO_AGEB, low_memory=False)
    
    # 💥 BLINDAJE 1: Estandarización de columnas (quita espacios fantasmas)
    df_censo.columns = df_censo.columns.astype(str).str.strip().str.upper()
    
    # Blindaje geográfico básico
    df_censo['ENTIDAD'] = pd.to_numeric(df_censo.get('ENTIDAD', 0), errors='coerce')
    df_censo['MUN'] = pd.to_numeric(df_censo.get('MUN', 0), errors='coerce')
    df_censo['MZA'] = pd.to_numeric(df_censo.get('MZA', 1), errors='coerce') # MZA 0 son los totales del AGEB
    
    df_toluca = df_censo[(df_censo['ENTIDAD'] == 15) & (df_censo['MUN'] == 106) & (df_censo['MZA'] == 0)].copy()
    
    if df_toluca.empty:
        print("  ⚠️ Advertencia: El filtro de Toluca (106) regresó vacío. Revisa la estructura del CSV.")
        return pd.DataFrame()

    # 💥 BLINDAJE 2: Escáner Semántico para evadir los cambios de nombre del INEGI
    # En lugar de hardcodear, buscamos coincidencias clave.
    col_nodren = next((c for c in df_toluca.columns if 'DREN' in c and ('NO' in c or 'SIN' in c)), None)
    col_pisotie = next((c for c in df_toluca.columns if 'PISOTI' in c), None)
    col_sintic = next((c for c in df_toluca.columns if 'SINTIC' in c or 'NOTIC' in c), None)
    col_vivtot = next((c for c in df_toluca.columns if 'VIVTOT' in c), None)
    col_pobtot = next((c for c in df_toluca.columns if 'POBTOT' in c), None)

    # 💥 BLINDAJE 3: Destructor universal de censura (convierte '*' en '0')
    def limpiar_serie(serie):
        if serie is None: return pd.Series(0, index=df_toluca.index)
        return pd.to_numeric(serie.astype(str).str.replace('*', '0', regex=False), errors='coerce').fillna(0)

    # Extraemos y limpiamos de forma ultra-segura
    vph_nodren = limpiar_serie(df_toluca[col_nodren] if col_nodren else None)
    vph_pisotie = limpiar_serie(df_toluca[col_pisotie] if col_pisotie else None)
    vph_sintic = limpiar_serie(df_toluca[col_sintic] if col_sintic else None)
    vivtot = limpiar_serie(df_toluca[col_vivtot] if col_vivtot else None)
    df_toluca['POBTOT_CLN'] = limpiar_serie(df_toluca[col_pobtot] if col_pobtot else None)

    # 📊 CÁLCULO DEL NIVEL DE PRECARIEDAD URBANA
    vivtot_safe = vivtot.replace(0, 1) # Evita división por cero si una manzana no tiene casas registradas
    
    pct_nodren = (vph_nodren / vivtot_safe) * 100
    pct_pisotie = (vph_pisotie / vivtot_safe) * 100
    pct_sintic = (vph_sintic / vivtot_safe) * 100
    
    # Modelo Matemático: Ponderación de Marginación
    df_toluca['INDICE_MARGINACION_AGEB'] = (pct_nodren * 0.4 + pct_pisotie * 0.4 + pct_sintic * 0.2).clip(upper=100)
    
    # Normalización para el Join Espacial
    df_toluca['CVE_AGEB'] = df_toluca.get('AGEB', pd.Series('0000', index=df_toluca.index)).astype(str).str.zfill(4)
    
    print("  [✅ Éxito] Censo procesado. Variables sociodemográficas extraídas limpiamente.")
    return df_toluca[['CVE_AGEB', 'INDICE_MARGINACION_AGEB', 'POBTOT_CLN']]

# ==============================================================================
# FASE 2: EL MOTOR GEOMÉTRICO (INTERPOLACIÓN AREAL SECCIÓN ↔ AGEB)
# ==============================================================================
def generar_mapa_sits():
    print("\n==================================================")
    print(" INICIANDO MOTOR ESPACIAL SITS (INTERPOLACIÓN AREAL)")
    print("==================================================")
    
    # 1. Cargar Bóveda Maestra
    if not os.path.exists(RUTA_MAESTRA_SITS):
        print("  ❌ Error Crítico: No se encontró el Cerebro Predictivo (CRUCE_MAESTRO_INDICADORES).")
        return
    df_maestro = pd.read_csv(RUTA_MAESTRA_SITS)
    df_maestro['SECCION'] = df_maestro['SECCION'].astype(str)
    
    # 2. Cargar Planimetría Vectorial
    print("  🗺️ Cargando Polígonos de Cartografía INE e INEGI...")
    if not os.path.exists(RUTA_SHP_INE) or not os.path.exists(RUTA_SHP_INEGI):
        print("  ❌ Error Cartográfico: Faltan shapefiles (.shp) en la carpeta 01_raw.")
        return
        
    gdf_secciones = gpd.read_file(RUTA_SHP_INE)
    gdf_agebs = gpd.read_file(RUTA_SHP_INEGI)
    
    # 💥 BLINDAJE INTELIGENTE: Extracción Forense del Municipio
    col_mun_ine = next((c for c in gdf_secciones.columns if 'MUN' in str(c).upper()), None)
    if col_mun_ine is None:
        print("  ❌ Error: El mapa del INE no tiene columna de Municipio.")
        return
        
    serie_mun = gdf_secciones[col_mun_ine]
    if isinstance(serie_mun, pd.DataFrame):
        col_correcta = next((serie_mun.iloc[:, i] for i in range(serie_mun.shape[1]) if serie_mun.iloc[:, i].astype(str).str.contains('106').any()), serie_mun.dropna(axis=1, how='all').iloc[:, 0])
        serie_mun = col_correcta

    filtro_toluca = pd.to_numeric(serie_mun, errors='coerce') == 106
    gdf_secciones = gdf_secciones[filtro_toluca].copy()
    
    # 💥 BLINDAJE INTELIGENTE: Extracción Forense de la Sección
    col_sec_ine = next((c for c in gdf_secciones.columns if 'SEC' in str(c).upper()), None)
    serie_sec = gdf_secciones[col_sec_ine]
    
    if isinstance(serie_sec, pd.DataFrame):
        col_correcta_sec = next((serie_sec.iloc[:, i] for i in range(serie_sec.shape[1]) if pd.to_numeric(serie_sec.iloc[:, i], errors='coerce').max() > 1000), serie_sec.dropna(axis=1, how='all').iloc[:, 0])
        serie_sec = col_correcta_sec

    # EXTRACCIÓN QUIRÚRGICA: Generar mapa virgen para evadir la tabla sucia del DBF
    print("  [🔪 Extracción Quirúrgica] Construyendo polígonos puros...")
    gdf_limpio = gpd.GeoDataFrame(geometry=gdf_secciones.geometry, crs=gdf_secciones.crs)
    gdf_limpio['SECCION'] = serie_sec.astype(str).str.replace('.0', '', regex=False)
    gdf_secciones = gdf_limpio

    # 3. Transformación a Metros Planos
    print("  📐 Proyectando Sistema de Coordenadas (EPSG:6372) para cálculo de áreas de precisión...")
    gdf_secciones = gdf_secciones.to_crs(epsg=6372)
    gdf_agebs = gdf_agebs.to_crs(epsg=6372)
    
    gdf_secciones['AREA_SEC_TOTAL'] = gdf_secciones.geometry.area
    
    # 4. Inyección Tabular a Vector
    df_marg = procesar_marginacion_ageb()
    if not df_marg.empty:
        # Escáner dinámico para la clave AGEB (a veces CVEGEO, a veces CVE_AGEB)
        col_cve_ageb = next((c for c in gdf_agebs.columns if 'AGEB' in str(c).upper() or 'CVEGEO' in str(c).upper()), None)
        if col_cve_ageb:
            gdf_agebs['AGEB_CORTA'] = gdf_agebs[col_cve_ageb].astype(str).str[-4:]
            gdf_agebs = gdf_agebs.merge(df_marg, left_on='AGEB_CORTA', right_on='CVE_AGEB', how='left').fillna(0)
        else:
            gdf_agebs['INDICE_MARGINACION_AGEB'] = 0
    else:
        print("  ⚠️ Alerta: El Censo regresó vacío. Marginación en 0.")
        gdf_agebs['INDICE_MARGINACION_AGEB'] = 0

    # 5. La Licuadora: Intersección Espacial Ponderada (Overlay)
    print("  🌪️ Realizando Intersección Geométrica (Sección ↔ AGEB)...")
    
    # Prevenimos cruces nulos por errores geométricos
    try:
        interseccion = gpd.overlay(gdf_secciones, gdf_agebs, how='intersection')
    except Exception as e:
        print(f"  ❌ Error en la intersección espacial: {e}")
        return

    if not interseccion.empty:
        # Matemáticas de Superficie
        interseccion['AREA_INTERSECTADA'] = interseccion.geometry.area
        interseccion['PESO_AREA'] = interseccion['AREA_INTERSECTADA'] / interseccion['AREA_SEC_TOTAL']
        
        # Índice de Frustración Urbana (IFU)
        interseccion['MARGINACION_PONDERADA'] = interseccion['INDICE_MARGINACION_AGEB'] * interseccion['PESO_AREA']
        
        print("  ⚖️ Agrupando Marginación Ponderada a nivel Seccional...")
        df_marginacion_seccion = interseccion.groupby('SECCION')['MARGINACION_PONDERADA'].sum().reset_index()
        df_marginacion_seccion = df_marginacion_seccion.rename(columns={'MARGINACION_PONDERADA': 'INDICE_IFU'})
    else:
        print("  ⚠️ La intersección regresó vacía. Asignando IFU = 0.")
        df_marginacion_seccion = pd.DataFrame({'SECCION': gdf_secciones['SECCION'], 'INDICE_IFU': 0})
    
    # 6. Fusión Definitiva
    print("  🔗 Fusionando Topología Urbana con el Histórico Electoral...")
    df_maestro_enriquecido = df_maestro.merge(df_marginacion_seccion, on='SECCION', how='left').fillna(0)
    
    # 7. Retorno a Sistema Global (WGS84 para Folium)
    gdf_final = gdf_secciones[['SECCION', 'geometry']].merge(df_maestro_enriquecido, on='SECCION', how='right')
    gdf_final = gdf_final.to_crs(epsg=4326)
    
    # 8. Renderización y Embalaje
    print(f"  💾 Exportando Mapa de Calor Táctico (GeoJSON) a: {RUTA_SALIDA_GEOJSON}")
    
    # Convertimos a GeoJSON. Si el archivo ya existe, lo sobrescribe.
    try:
        gdf_final.to_file(RUTA_SALIDA_GEOJSON, driver='GeoJSON')
        print("  ✅ Empalme Espacial Terminado con Éxito. El Cuarto de Guerra está listo para desplegar el mapa.")
    except Exception as e:
        print(f"  ❌ Error al guardar el GeoJSON: {e}")

if __name__ == '__main__':
    generar_mapa_sits()