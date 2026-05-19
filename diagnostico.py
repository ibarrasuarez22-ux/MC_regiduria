import json
import pandas as pd
import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ruta_datos = os.path.join(BASE_DIR, 'data', '03_processed', 'CRUCE_MAESTRO_INDICADORES.csv')
ruta_mapa = os.path.join(BASE_DIR, 'data', '03_processed', 'MAPA_TOLUCA_SITS.geojson')

try:
    df = pd.read_csv(ruta_datos)
    print("✅ CSV Cargado.")
    print("▶️ Primeras 5 secciones en tu EXCEL:")
    print(df['SECCION'].head(5).tolist())
    print("-" * 40)
    
    with open(ruta_mapa, 'r', encoding='utf-8') as f:
        geo = json.load(f)
    print("✅ GeoJSON Cargado.")
    print("▶️ Primeras 5 secciones en tu MAPA:")
    # Extraemos cómo se llama realmente la llave en el mapa
    muestras = []
    for feature in geo['features'][:5]:
        muestras.append(feature['properties'].get('SECCION', 'No existe la columna SECCION'))
    print(muestras)
    
except Exception as e:
    print(f"❌ Error al leer los archivos: {e}")