import pandas as pd
import numpy as np
import os
import re
import glob
import warnings

# Apagamos advertencias de rendimiento de Pandas
warnings.filterwarnings('ignore', category=pd.errors.PerformanceWarning)
warnings.filterwarnings('ignore')

# ==============================================================================
# CONFIGURACIÓN DE RUTAS 
# ==============================================================================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
RAW_ELEC_DIR = os.path.join(BASE_DIR, 'data', '01_raw', 'electoral')
RAW_PADRON_DIR = os.path.join(BASE_DIR, 'data', '01_raw', 'padron')
INTERIM_DIR = os.path.join(BASE_DIR, 'data', '02_interim')

os.makedirs(INTERIM_DIR, exist_ok=True)

# ==============================================================================
# 1. MOTORES DE LIMPIEZA FORENSE E INTELIGENCIA
# ==============================================================================
def leer_archivo_robusto(ruta):
    """EL ESCÁNER INTELIGENTE: Encuentra el encabezado real y aplana columnas."""
    try:
        try: temp_df = pd.read_csv(ruta, header=None, encoding='utf-8', nrows=30, on_bad_lines='skip', dtype=str)
        except: temp_df = pd.read_csv(ruta, header=None, encoding='latin1', nrows=30, on_bad_lines='skip', dtype=str)
        
        header_idx = 0
        for idx, row in temp_df.iterrows():
            row_str = ' '.join(row.fillna('').astype(str)).upper()
            if 'SECCION' in row_str or 'SECCIÓN' in row_str or 'SEC' in row_str:
                header_idx = idx
                break
        
        try: df = pd.read_csv(ruta, skiprows=header_idx, encoding='utf-8', on_bad_lines='skip')
        except: df = pd.read_csv(ruta, skiprows=header_idx, encoding='latin1', on_bad_lines='skip')
            
        # APLANADORA ABSOLUTA: Forzar que todo encabezado sea un texto simple (mata las tuplas)
        df.columns = [str(c).strip().upper() for c in df.columns]
        
        # Eliminar columnas duplicadas
        df = df.loc[:, ~df.columns.duplicated()].copy()
        return df
    except Exception as e:
        print(f"  ❌ Error escaneando {os.path.basename(ruta)}: {e}")
        return pd.DataFrame()

def limpiar_dato_numerico(x):
    """Mata comas, % y errores."""
    if pd.isna(x): return 0.0
    if isinstance(x, (int, float)): return float(x)
    x_str = str(x).strip().upper()
    x_str = x_str.replace(',', '').replace('%', '').replace(' ', '')
    if x_str in ['FALSO', '#¡CALC!', '#DIV/0!', '*', '-', '', 'ND', 'N/A']: return 0.0
    try: return float(x_str)
    except: return 0.0

def aislar_toluca(df):
    """Unifica 106 y 107 para Toluca."""
    col_mun = None
    for c in df.columns:
        if 'MUNICIPIO' in c or 'MUN' in c or 'CABECERA' in c:
            col_mun = c
            break
            
    if not col_mun: return df
    
    serie_mun = df[col_mun]
    if isinstance(serie_mun, pd.DataFrame):
        serie_mun = serie_mun.iloc[:, 0]
        
    mask = serie_mun.astype(str).str.contains('106|107|TOLUCA', case=False, na=False)
    return df[mask].copy()

def blindar_secciones(df):
    """Mata la basura del final de Excel y limpia la sección (Sin usar DROP)."""
    col_sec = None
    for c in df.columns:
        if c in ['SECCION', 'SECCIÓN', 'SEC']:
            col_sec = c
            break
            
    if not col_sec: return df
    
    serie_sec = df[col_sec]
    if isinstance(serie_sec, pd.DataFrame):
        serie_sec = serie_sec.iloc[:, 0]
        
    # Limpiamos los datos
    serie_sec_limpia = serie_sec.apply(limpiar_dato_numerico)
    
    # SOBRESCRIBIMOS sin borrar nada para evitar KeyErrors
    df = df.copy()
    df[col_sec] = serie_sec_limpia
    
    # Filtramos
    df = df[(df[col_sec] > 0) & (df[col_sec] <= 9999)].copy()
    df[col_sec] = df[col_sec].astype(int)
    
    # Renombramos al estándar
    if col_sec != 'SECCION':
        df.rename(columns={col_sec: 'SECCION'}, inplace=True)
        
    return df

def sumar_columnas(df, palabras_clave):
    cols_encontradas = [c for c in df.columns if any(p in c for p in palabras_clave)]
    if not cols_encontradas: return 0.0
    
    df_subset = df[cols_encontradas].copy()
    for col in df_subset.columns:
        if isinstance(df_subset[col], pd.DataFrame):
            df_subset[col] = df_subset[col].iloc[:, 0]
        df_subset[col] = df_subset[col].apply(limpiar_dato_numerico)
    return df_subset.sum(axis=1)

def obtener_columna(df, posibles_nombres):
    for nombre in posibles_nombres:
        for col in df.columns:
            if col == nombre.upper(): return col
    return None

# ==============================================================================
# FASE 1A: PADRÓN ELECTORAL 2026 
# ==============================================================================
def procesar_padron_2026():
    print("\n▶ [FASE 1A] PROCESANDO PADRÓN 2026...")
    lista_archivos = [f for f in os.listdir(RAW_PADRON_DIR) if f.endswith('.csv') and 'INE_SECCION' not in f]
    
    if len(lista_archivos) == 0: 
        print("  ❌ No se encontró el Padrón.")
        return
    
    archivo_objetivo = lista_archivos
    if isinstance(archivo_objetivo, list): archivo_objetivo = archivo_objetivo
    archivo_objetivo = str(archivo_objetivo).replace("['", "").replace("']", "").strip()
    
    ruta = os.path.join(RAW_PADRON_DIR, archivo_objetivo)
    print(f"  📄 Escaneando y limpiando: {archivo_objetivo}...")
    
    df = leer_archivo_robusto(ruta)
    if df.empty: return
    
    df = aislar_toluca(df)
    df = blindar_secciones(df)
    
    if df.empty:
        print("  ⚠️ El Padrón quedó vacío tras filtrar Toluca.")
        return

    mapeo = {
        'LISTA': 'LISTA_NOMINAL_2026', 
        'LISTA_HOMBRES': 'HOMBRES_2026', 
        'LISTA_MUJERES': 'MUJERES_2026',
        'PADRON': 'PADRON_2026'
    }
    
    for col_orig, col_nueva in mapeo.items():
        if col_orig in df.columns:
            serie = df[col_orig]
            if isinstance(serie, pd.DataFrame): serie = serie.iloc[:, 0]
            df[col_nueva] = serie.apply(limpiar_dato_numerico)

    if 'HOMBRES_2026' not in df.columns: df['HOMBRES_2026'] = 0
    if 'MUJERES_2026' not in df.columns: df['MUJERES_2026'] = 0

    df_final = df[['SECCION', 'LISTA_NOMINAL_2026', 'HOMBRES_2026', 'MUJERES_2026']].copy()
    ruta_salida = os.path.join(INTERIM_DIR, 'TOLUCA_PADRON_2026_LIMPIO.csv')
    df_final.to_csv(ruta_salida, index=False)
    print(f"  ✅ Padrón Recuperado: {len(df_final)} secciones.")

# ==============================================================================
# FASE 1B: HISTÓRICOS MUNICIPALES
# ==============================================================================
def limpiar_datos_municipales():
    print("\n▶ [FASE 1B] NORMALIZANDO HISTÓRICOS MUNICIPALES...")
    archivos_elec = glob.glob(os.path.join(RAW_ELEC_DIR, 'ELECTORAL_*.csv'))
    dataframes_limpios = []
    
    for archivo in archivos_elec:
        anio_match = re.search(r'\d{4}', os.path.basename(archivo))
        if not anio_match: continue
        anio = int(anio_match.group())
        print(f"  ⚙️ Procesando: {os.path.basename(archivo)}...")
        
        df_tol = leer_archivo_robusto(archivo)
        if df_tol.empty: continue
        
        df_tol = aislar_toluca(df_tol)
        df_tol = blindar_secciones(df_tol)
        if df_tol.empty: continue
            
        for col in df_tol.columns:
            if col != 'SECCION':
                serie = df_tol[col]
                if isinstance(serie, pd.DataFrame): serie = serie.iloc[:, 0]
                df_tol[col] = serie.apply(limpiar_dato_numerico)

        col_votos = obtener_columna(df_tol, ['TOTAL_VOTOS', 'TOTAL', 'TOTAL VOTOS'])
        col_lista = obtener_columna(df_tol, ['LISTA_NOMINAL', 'LISTA NOMINAL'])
        
        df_tol['VOTOS_TOTALES'] = df_tol[col_votos] if col_votos else 0
        df_tol['LISTA_NOMINAL_HIST'] = df_tol[col_lista] if col_lista else 0
        
        df_tol['ESTRUCTURA_TRADICIONAL'] = sumar_columnas(df_tol, ['PRI', 'PAN', 'PRD', 'NAEM', 'NA', 'NVA_ALIANZA'])
        df_tol['ESTRUCTURA_IZQUIERDA'] = sumar_columnas(df_tol, ['MORENA', 'PT', 'PVEM'])
        df_tol['VOTOS_MC'] = sumar_columnas(df_tol, ['MC', 'MOVIMIENTO CIUDADANO', 'M C'])
        df_tol['VOTOS_NULOS'] = sumar_columnas(df_tol, ['NULO', 'NUM_VOTOS_NULOS', 'NULOS'])
        
        df_est = df_tol[['SECCION', 'ESTRUCTURA_TRADICIONAL', 'ESTRUCTURA_IZQUIERDA', 'VOTOS_MC', 'VOTOS_NULOS', 'VOTOS_TOTALES', 'LISTA_NOMINAL_HIST']].copy()
        df_est['ANIO'] = anio
        dataframes_limpios.append(df_est)
        
    if dataframes_limpios:
        df_final = pd.concat(dataframes_limpios, ignore_index=True)
        df_final = df_final.groupby(['SECCION', 'ANIO']).sum().reset_index()
        df_final['PARTICIPACION_PCT'] = np.where(df_final['LISTA_NOMINAL_HIST'] > 0, (df_final['VOTOS_TOTALES'] / df_final['LISTA_NOMINAL_HIST']) * 100, 0).round(2)
        ruta_salida = os.path.join(INTERIM_DIR, 'TOLUCA_MUNICIPAL_LIMPIO.csv')
        df_final.to_csv(ruta_salida, index=False)
        print(f"  ✅ Guardado: {ruta_salida}")

# ==============================================================================
# FASE 1C: HISTÓRICOS ESTATALES (CON EXTRACCIÓN DE VOTOS TOTALES)
# ==============================================================================
def limpiar_datos_estatales():
    print("\n▶ [FASE 1C] NORMALIZANDO GUBERNATURAS...")
    archivos = [f for f in os.listdir(RAW_ELEC_DIR) if 'GOB' in f.upper() or '2017' in f or '2023' in f]
    dataframes_limpios = []
    
    for arch in archivos:
        print(f"  ⚙️ Procesando estatal: {arch}...")
        ruta = os.path.join(RAW_ELEC_DIR, arch)
        
        df_tol = leer_archivo_robusto(ruta)
        if df_tol.empty: continue
        
        df_tol = aislar_toluca(df_tol)
        df_tol = blindar_secciones(df_tol)
        if df_tol.empty: continue

        for col in df_tol.columns:
            if col != 'SECCION':
                serie = df_tol[col]
                if isinstance(serie, pd.DataFrame): serie = serie.iloc[:, 0]
                df_tol[col] = serie.apply(limpiar_dato_numerico)

        # 🎯 AUDITORÍA: Extraemos los Totales y la Lista del IEEM Estatal
        col_votos = obtener_columna(df_tol, ['TOTAL_VOTOS', 'TOTAL', 'TOTAL VOTOS'])
        col_lista = obtener_columna(df_tol, ['LISTA_NOMINAL', 'LISTA NOMINAL'])
        
        df_tol['VOTOS_TOTALES'] = df_tol[col_votos] if col_votos else 0
        df_tol['LISTA_NOMINAL_HIST'] = df_tol[col_lista] if col_lista else 0

        df_tol['ESTRUCTURA_TRADICIONAL'] = sumar_columnas(df_tol, ['PRI', 'PAN', 'PRD', 'NAEM', 'NA', 'NVA_ALIANZA'])
        df_tol['ESTRUCTURA_IZQUIERDA'] = sumar_columnas(df_tol, ['MORENA', 'PT', 'PVEM'])
        
        anio = 2023 if '2023' in arch else 2017
        df_est = df_tol[['SECCION', 'ESTRUCTURA_TRADICIONAL', 'ESTRUCTURA_IZQUIERDA', 'VOTOS_TOTALES', 'LISTA_NOMINAL_HIST']].copy()
        df_est['ANIO'] = anio
        df_est['TIPO_ELECCION'] = 'GUBERNATURA'
        dataframes_limpios.append(df_est)

    if dataframes_limpios:
        df_final = pd.concat(dataframes_limpios, ignore_index=True)
        df_final = df_final.groupby(['SECCION', 'ANIO', 'TIPO_ELECCION']).sum().reset_index()
        ruta_salida = os.path.join(INTERIM_DIR, 'TOLUCA_ESTATAL_LIMPIO.csv')
        df_final.to_csv(ruta_salida, index=False)
        print(f"  ✅ Guardado: {ruta_salida}\n")

# ==============================================================================
# EJECUCIÓN 
# ==============================================================================
if __name__ == '__main__':
    print("==================================================")
    print(" INICIANDO ETL: SISTEMA DE INTELIGENCIA SITS (FASE 1)")
    print("==================================================")
    procesar_padron_2026()
    limpiar_datos_municipales()
    limpiar_datos_estatales()
    print("✅ FASE 1 COMPLETADA")