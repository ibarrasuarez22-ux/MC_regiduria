import os
import time
import json
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import warnings

# Ignorar advertencias menores de proyecciones
warnings.filterwarnings('ignore')

# ==============================================================================
# CONFIGURACIÓN DE RUTAS (ARQUITECTURA SITS)
# ==============================================================================
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
INTERIM_DIR = os.path.join(BASE_DIR, 'data', '02_interim')
RAW_CARTOGRAFIA_DIR = os.path.join(BASE_DIR, 'data', '01_raw', 'cartografia_ine')

ruta_shp = os.path.join(RAW_CARTOGRAFIA_DIR, 'SECCION.shp')
ruta_salida = os.path.join(INTERIM_DIR, 'INSATISFACCION_POR_SECCION.csv')

# ==============================================================================
# MOTOR DE EXTRACCIÓN SIGILOSO (SELENIUM)
# ==============================================================================
def extraer_baches_waze():
    print("==================================================")
    print(" INICIANDO RADAR SITS: INSATISFACCION EN TIEMPO REAL")
    print("==================================================")
    print("  ⚙️ Levantando navegador fantasma para evadir bloqueos 403...")
    
    # Configuración de Chrome en modo Headless (Invisible)
    chrome_options = Options()
    chrome_options.add_argument("--headless") 
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    # Activar la captura de tráfico de red
    chrome_options.set_capability("goog:loggingPrefs", {"performance": "ALL"})

    try:
        driver = webdriver.Chrome(options=chrome_options)
    except Exception as e:
        print(f"  ❌ Error: No se pudo iniciar Chrome. ¿Tienes instalado ChromeDriver o Selenium 4? Error: {e}")
        return pd.DataFrame()

    # Coordenadas de la caja que enmarca a Toluca
    lat = "19.2844"
    lon = "-99.6404"
    zoom = "13" 
    url_toluca = f"https://www.waze.com/live-map/directions?latlng={lat}%2C{lon}&zoom={zoom}"

    print("  🌐 Conectando a los servidores de tráfico...")
    driver.get(url_toluca)

    # Esperar a que la página cargue y resuelva los tokens de seguridad de Google (reCAPTCHA)
    time.sleep(10) 
    
    # Simular interacción humana para forzar la petición
    driver.execute_script("window.scrollBy(0, 500);")
    time.sleep(3)

    # Extraer los registros de red capturados en memoria
    logs = driver.get_log("performance")
    alertas_procesadas = []

    print("  🕵️‍♂️ Interceptando API oculta y desencriptando alertas...")
    for entry in logs:
        try:
            log = json.loads(entry["message"])["message"]
            
            if "Network.responseReceived" in log["method"]:
                params = log.get("params", {})
                response = params.get("response", {})
                req_url = response.get("url", "")
                
                # Buscar el archivo GeoRSS que contiene los baches
                if "api/georss" in req_url or "rtserver" in req_url:
                    request_id = params["requestId"]
                    
                    try:
                        # Extraer JSON directamente de la memoria del navegador
                        body = driver.execute_cdp_cmd("Network.getResponseBody", {"requestId": request_id})
                        datos_waze = json.loads(body["body"])
                        
                        # Extraer alertas
                        for alerta in datos_waze.get("alerts", []):
                            tipo_alerta = alerta.get("subtype", "") or alerta.get("type", "")
                            
                            # Filtro predictivo: Solo nos importan los baches y peligros en la vía
                            if "POT_HOLE" in tipo_alerta or "HAZARD" in tipo_alerta:
                                alertas_procesadas.append({
                                    "id_reporte": alerta.get("uuid"),
                                    "tipo": tipo_alerta,
                                    "calle": alerta.get("street", "Desconocida"),
                                    "lat": alerta["location"]["y"],
                                    "lon": alerta["location"]["x"]
                                })
                    except:
                        continue
        except:
            continue

    driver.quit()
    
    if alertas_procesadas:
        df_alertas = pd.DataFrame(alertas_procesadas)
        df_alertas = df_alertas.drop_duplicates(subset=['id_reporte'])
        print(f"  ✅ Extracción exitosa: Se encontraron {len(df_alertas)} focos de insatisfacción hoy.")
        return df_alertas
    else:
        print("  ⚠️ No se interceptaron alertas activas en esta ventana de tiempo.")
        return pd.DataFrame()

# ==============================================================================
# EMPALME ESPACIAL (DE COORDENADAS A SECCIONES ELECTORALES)
# ==============================================================================
def calcular_indice_insatisfaccion(df_alertas):
    if df_alertas.empty:
        return
        
    print("  🗺️ Realizando Cruce Espacial (Baches -> Secciones Electorales)...")
    
    if not os.path.exists(ruta_shp):
        print(f"  ❌ Error: No se encuentra el shapefile en {ruta_shp}")
        return

    # 1. Convertir la tabla de Pandas a un Mapa de Puntos (GeoPandas)
    geometria = [Point(xy) for xy in zip(df_alertas['lon'], df_alertas['lat'])]
    gdf_alertas = gpd.GeoDataFrame(df_alertas, geometry=geometria, crs="EPSG:4326")

    # 2. Cargar el Mapa de Secciones del INE
    mapa_secciones = gpd.read_file(ruta_shp)
    col_sec_mapa = [c for c in mapa_secciones.columns if c.upper() == 'SECCION']
    if col_sec_mapa:
        mapa_secciones = mapa_secciones.rename(columns={col_sec_mapa[0]: 'SECCION'})
    
    # Estandarizar proyecciones para que los mapas empalmen matemáticamente
    mapa_secciones = mapa_secciones.to_crs(epsg=4326)

    # 3. Spatial Join (El empalme maestro)
    # Asigna a cada bache la sección en la que cae
    baches_por_seccion = gpd.sjoin(gdf_alertas, mapa_secciones, how="inner", predicate='intersects')

    # 4. Agrupar y Contar (Creación de la métrica)
    indice_seccion = baches_por_seccion.groupby('SECCION').size().reset_index(name='TOTAL_QUEJAS_ACTIVAS')
    
    # Normalizar sección como texto limpio
    indice_seccion['SECCION'] = indice_seccion['SECCION'].astype(str).str.replace('.0', '', regex=False).str.strip()

    # 5. Guardar en la carpeta Interim para que lo consuma el Streamlit
    indice_seccion.to_csv(ruta_salida, index=False)
    
    print(f"  ✅ Cruce finalizado. Índice de Insatisfacción guardado en: {ruta_salida}")
    print("==================================================")


if __name__ == '__main__':
    datos_crudos = extraer_baches_waze()
    calcular_indice_insatisfaccion(datos_crudos)