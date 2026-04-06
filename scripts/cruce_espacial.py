import pandas as pd
import geopandas as gpd
import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW_CARTOGRAFIA_DIR = os.path.join(BASE_DIR, 'data', '01_raw', 'cartografia_ine')
PROCESSED_DIR = os.path.join(BASE_DIR, 'data', '03_processed')
INTERIM_DIR = os.path.join(BASE_DIR, 'data', '02_interim')
ASSETS_DIR = os.path.join(BASE_DIR, 'app', 'assets')

ruta_maestro = os.path.join(PROCESSED_DIR, 'CRUCE_MAESTRO_INDICADORES.csv')
ruta_shp = os.path.join(RAW_CARTOGRAFIA_DIR, 'SECCION.shp')
ruta_waze = os.path.join(INTERIM_DIR, 'INSATISFACCION_POR_SECCION.csv')

def fusionar_espacial():
    print("▶ Iniciando Empalme Cartográfico...")
    
    df_electoral = pd.read_csv(ruta_maestro)
    df_electoral['SECCION'] = df_electoral['SECCION'].astype(str).str.replace(r'\.0$', '', regex=True).str.strip()

    # Añadir Waze al mapa
    if os.path.exists(ruta_waze):
        df_waze = pd.read_csv(ruta_waze)
        df_waze['SECCION'] = df_waze['SECCION'].astype(str).str.replace(r'\.0$', '', regex=True).str.strip()
        df_electoral = pd.merge(df_electoral, df_waze[['SECCION', 'TOTAL_QUEJAS_ACTIVAS']], on='SECCION', how='left').fillna(0)
    else:
        df_electoral['TOTAL_QUEJAS_ACTIVAS'] = 0

    mapa = gpd.read_file(ruta_shp).to_crs(epsg=4326)
    col_sec = [c for c in mapa.columns if c.upper() in ['SECCION', 'SECCIÓN', 'SEC']][0]
    mapa['SECCION'] = mapa[col_sec].astype(str).str.replace(r'\.0$', '', regex=True).str.strip()

    mapa_final = mapa.merge(df_electoral, on='SECCION', how='inner')
    
    # Limpiar columnas pesadas
    cols_drop = [c for c in ['geometry_y', 'index_right', 'FID'] if c in mapa_final.columns]
    mapa_final = mapa_final.drop(columns=cols_drop, errors='ignore')
    
    mapa_final.to_file(os.path.join(ASSETS_DIR, 'MAPA_ESTRATEGICO_TOLUCA.geojson'), driver='GeoJSON')
    print("  ✅ Mapa listo para visualización.")

if __name__ == '__main__':
    fusionar_espacial()