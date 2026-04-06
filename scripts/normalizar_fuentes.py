import pandas as pd
import numpy as np
import os
import re
import glob # <-- Añadido para buscar archivos dinámicamente

# ==============================================================================
# CONFIGURACIÓN DE RUTAS (ARQUITECTURA DE CLASE MUNDIAL)
# ==============================================================================
# BASE_DIR apunta a la carpeta raíz SISTEMA_ELECTORAL_TOLUCA
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Rutas de entrada (Bóveda intocable)
RAW_ELEC_DIR = os.path.join(BASE_DIR, 'data', '01_raw', 'electoral')
RAW_PADRON_DIR = os.path.join(BASE_DIR, 'data', '01_raw', 'padron')

# Ruta de salida temporal
INTERIM_DIR = os.path.join(BASE_DIR, 'data', '02_interim')

# Asegurarnos de que la carpeta de salida exista
os.makedirs(INTERIM_DIR, exist_ok=True)

# ==============================================================================
# FUNCIONES DE EXTRACCIÓN Y TRANSFORMACIÓN (ETL)
# ==============================================================================

def obtener_columna(df, posibles_nombres):
    """Busca una columna iterando sobre una lista de posibles nombres históricos."""
    for nombre in posibles_nombres:
        # Búsqueda exacta ignorando mayúsculas/minúsculas
        col = df.columns[df.columns.str.upper() == nombre.upper()]
        if not col.empty:
            return col[0]
    return None

def procesar_padron_2026():
    """Ingesta y normalización del Padrón y Lista Nominal al 2026 con Inteligencia Demográfica."""
    print("\n▶ Iniciando extracción del Horizonte 2026 (Lista Nominal y Género)...")
    
    # --------------------------------------------------------------------------
    # MEJORA DE CLASE MUNDIAL: Búsqueda dinámica de archivos (CSV o Excel)
    # --------------------------------------------------------------------------
    archivos_excel = glob.glob(os.path.join(RAW_PADRON_DIR, '*.xlsx')) + glob.glob(os.path.join(RAW_PADRON_DIR, '*.xls'))
    archivos_csv = glob.glob(os.path.join(RAW_PADRON_DIR, '*.csv'))
    
    ruta_padron = None
    es_excel = False
    
    if archivos_excel:
        ruta_padron = archivos_excel[0]
        es_excel = True
        print(f"  📄 Archivo EXCEL detectado: {os.path.basename(ruta_padron)}")
    elif archivos_csv:
        ruta_padron = archivos_csv[0]
        print(f"  📄 Archivo CSV detectado: {os.path.basename(ruta_padron)}")
    else:
        print(f"  ⚠️ Advertencia: No se encontró ningún archivo CSV o Excel del padrón 2026 en {RAW_PADRON_DIR}")
        return None
        
    try:
        if es_excel:
            df_padron = pd.read_excel(ruta_padron)
            if not any(col.upper() in ['MUNICIPIO', 'SECCION', 'SECCIÓN', 'LISTA NOMINAL'] for col in df_padron.columns.astype(str)):
                df_padron = pd.read_excel(ruta_padron, skiprows=10)
        else:
            # Lectura en 'latin1' por el formato del INE
            df_padron = pd.read_csv(ruta_padron, skiprows=10, encoding='latin1', on_bad_lines='skip')
            
        # Asegurar mayúsculas para las columnas
        df_padron.columns = df_padron.columns.astype(str).str.upper().str.strip()
        
        # Filtrar Municipio Toluca (Clave 106)
        if 'MUNICIPIO' in df_padron.columns:
            # En el padrón del INE, Toluca es el municipio 106
            if pd.api.types.is_numeric_dtype(df_padron['MUNICIPIO']):
                df_toluca = df_padron[df_padron['MUNICIPIO'] == 106].copy()
            else:
                df_toluca = df_padron[df_padron['MUNICIPIO'].astype(str) == '106'].copy()
        elif 'NOMBRE_MUNICIPIO' in df_padron.columns:
            df_toluca = df_padron[df_padron['NOMBRE_MUNICIPIO'].astype(str).str.contains('TOLUCA', case=False, na=False)].copy()
        else:
            df_toluca = df_padron.copy() 

        col_sec = obtener_columna(df_toluca, ['SECCION', 'SECCIÓN', 'SEC'])
        # Añadimos la palabra 'LISTA' a nuestro radar de búsqueda
        col_ln = obtener_columna(df_toluca, ['LISTA', 'LISTA NOMINAL', 'LISTA_NOMINAL', 'LN', 'TOTAL_LN', 'PADRON'])
        
        # AUMENTO DEMOGRÁFICO: Búsqueda de columnas de género
        col_hom = obtener_columna(df_toluca, ['LISTA_HOMBRES', 'HOMBRES'])
        col_muj = obtener_columna(df_toluca, ['LISTA_MUJERES', 'MUJERES'])

        if col_sec and col_ln:
            columnas_a_extraer = [col_sec, col_ln]
            nombres_nuevos = {col_sec: 'SECCION', col_ln: 'LISTA_NOMINAL_2026'}
            
            if col_hom:
                columnas_a_extraer.append(col_hom)
                nombres_nuevos[col_hom] = 'HOMBRES_2026'
            if col_muj:
                columnas_a_extraer.append(col_muj)
                nombres_nuevos[col_muj] = 'MUJERES_2026'
                
            df_final = df_toluca[columnas_a_extraer].copy()
            df_final.rename(columns=nombres_nuevos, inplace=True)
            
            df_final['SECCION'] = pd.to_numeric(df_final['SECCION'], errors='coerce')
            df_final['LISTA_NOMINAL_2026'] = pd.to_numeric(df_final['LISTA_NOMINAL_2026'], errors='coerce').fillna(0)
            
            if 'HOMBRES_2026' in df_final.columns:
                df_final['HOMBRES_2026'] = pd.to_numeric(df_final['HOMBRES_2026'], errors='coerce').fillna(0)
            if 'MUJERES_2026' in df_final.columns:
                df_final['MUJERES_2026'] = pd.to_numeric(df_final['MUJERES_2026'], errors='coerce').fillna(0)
            
            ruta_salida = os.path.join(INTERIM_DIR, 'TOLUCA_PADRON_2026_LIMPIO.csv')
            df_final.to_csv(ruta_salida, index=False)
            
            resumen = f"Total electores: {int(df_final['LISTA_NOMINAL_2026'].sum()):,}"
            if 'MUJERES_2026' in df_final.columns:
                resumen += f" | Mujeres: {int(df_final['MUJERES_2026'].sum()):,} | Hombres: {int(df_final['HOMBRES_2026'].sum()):,}"
            print(f"  ✅ Padrón 2026 procesado y guardado en {ruta_salida}. {resumen}")
            
            return df_final
        else:
            print(f"  ❌ Error: No se encontraron las columnas. Columnas detectadas: {list(df_toluca.columns)}")
            return None
    except Exception as e:
        print(f"  ❌ Error fatal al procesar el padrón: {e}")
        return None

def limpiar_datos_municipales():
    print("\n▶ Iniciando limpieza y cálculo de series de tiempo municipales...")
    archivos = {
        2012: 'ELECTORAL_2012.csv',
        2015: 'ELECTORAL_2015.csv',
        2018: 'ELECTORAL_2018.csv',
        2021: 'ELECTORAL_2021.csv',
        2024: 'ELECTORAL_2024.csv'
    }
    
    dataframes_limpios = []

    for anio, nombre_archivo in archivos.items():
        ruta_archivo = os.path.join(RAW_ELEC_DIR, nombre_archivo)
        
        if not os.path.exists(ruta_archivo):
            print(f"  ⚠️ Advertencia: No se encontró {nombre_archivo} en {RAW_ELEC_DIR}")
            continue

        try:
            df = pd.read_csv(ruta_archivo, encoding='utf-8', low_memory=False)
        except UnicodeDecodeError:
            df = pd.read_csv(ruta_archivo, encoding='latin1', low_memory=False)
            
        # Homologar columna de Sección
        col_sec = obtener_columna(df, ['SECCIÓN', 'SECCION', 'SEC'])
        if col_sec:
            df.rename(columns={col_sec: 'SECCION'}, inplace=True)
            
        # Aislar Toluca (Manejo robusto para evitar colisiones con otros municipios)
        col_mun = obtener_columna(df, ['MUNICIPIO', 'NOMBRE_MUNICIPIO', 'CABECERA MUNICIPAL'])
        if col_mun:
            df = df[df[col_mun].astype(str).str.contains('TOLUCA', case=False, na=False)].copy()

        # Limpiar texto corrupto de las celdas numéricas
        columnas_numericas = df.columns.drop([col for col in ['MunicIpio', 'Cabecera Municipal', 'SECCION', 'Municipio', 'CASILLAS'] if col in df.columns], errors='ignore')
        for col in columnas_numericas:
            df[col] = pd.to_numeric(df[col].astype(str).str.replace(',', ''), errors='coerce').fillna(0)
            
        # --- CÁLCULO DE MÉTRICAS PREDICTIVAS ---
        col_total = obtener_columna(df, ['TOTAL', 'TOTAL_VOTOS', 'TOTAL VOTOS'])
        df['VOTOS_TOTALES'] = df[col_total] if col_total else 0
        
        col_ln = obtener_columna(df, ['LISTA_NOMINAL', 'LISTA NOMINAL', 'LN'])
        df['LISTA_NOMINAL_HIST'] = df[col_ln] if col_ln else np.nan
        
        col_nulos = obtener_columna(df, ['NULOS', 'VOTOS NULOS', 'VOTOS_NULOS', 'NO REGISTRADOS'])
        df['VOTOS_NULOS'] = df[col_nulos] if col_nulos else 0
            
        # --- NORMALIZACIÓN DE VOTOS MC ---
        if anio == 2012:
            df['VOTOS_MC'] = df.get('M C', 0) + (df.get('PRD-PT-MC', 0) / 3) + (df.get('PRD-MC', 0) / 2) + (df.get('PT-MC', 0) / 2)
        elif anio == 2018:
            df['VOTOS_MC'] = df.get('MC', 0) + (df.get('PAN_PRD_MC', 0) / 3) + (df.get('PAN_MC', 0) / 2) + (df.get('PRD_MC', 0) / 2)
        else:
            df['VOTOS_MC'] = df.get('MC', 0)
            
        # Extraer variables para el modelo SITS
        df_estrategico = df[['SECCION', 'VOTOS_MC', 'VOTOS_TOTALES', 'VOTOS_NULOS', 'LISTA_NOMINAL_HIST']].copy()
        
        # Cálculo de Apatía (Abstencionismo)
        df_estrategico['ABSTENCIONISMO_HIST'] = df_estrategico['LISTA_NOMINAL_HIST'] - df_estrategico['VOTOS_TOTALES']
        # Evitar números negativos por errores del INE
        df_estrategico['ABSTENCIONISMO_HIST'] = df_estrategico['ABSTENCIONISMO_HIST'].clip(lower=0) 
        
        df_estrategico['ANIO'] = anio
        df_estrategico['TIPO_ELECCION'] = 'MUNICIPAL'
        
        dataframes_limpios.append(df_estrategico)

    if dataframes_limpios:
        df_municipal_historico = pd.concat(dataframes_limpios, ignore_index=True)
        # Agrupar por sección y año para eliminar casillas duplicadas (Extra Básico/Contigua)
        df_municipal_historico = df_municipal_historico.groupby(['SECCION', 'ANIO', 'TIPO_ELECCION']).sum().reset_index()
        
        ruta_salida = os.path.join(INTERIM_DIR, 'TOLUCA_MUNICIPAL_LIMPIO.csv')
        df_municipal_historico.to_csv(ruta_salida, index=False)
        print(f"  ✅ Completado: Guardado en {ruta_salida}")
    else:
        print("  ❌ Error: No se pudo procesar ningún archivo municipal.")

def limpiar_datos_estatales():
    print("\n▶ Iniciando limpieza de métricas de Gobernatura...")
    
    archivos_estatales = {
        2017: 'Res_Definitivos_Gobernador_2017.csv',
        2023: '2023_SEE_GOB_MEX_SEC.csv'
    }

    dataframes_limpios = []

    for anio, file_name in archivos_estatales.items():
        ruta = os.path.join(RAW_ELEC_DIR, file_name)
        if not os.path.exists(ruta):
            print(f"  ⚠️ Advertencia: No se encontró el archivo {anio} en {ruta}")
            continue
            
        try:
            df = pd.read_csv(ruta, encoding='utf-8', low_memory=False)
        except:
            df = pd.read_csv(ruta, encoding='latin1', low_memory=False)

        col_mun = obtener_columna(df, ['MUNICIPIO', 'NOMBRE_MUNICIPIO'])
        if col_mun:
            df_tol = df[df[col_mun].astype(str).str.contains('TOLUCA', case=False, na=False)].copy()
        else:
            df_tol = df.copy()

        # Convertir a numérico limpiando comas
        cols_num = df_tol.columns.drop([col for col in ['SECCION', col_mun] if col in df_tol.columns], errors='ignore')
        for col in cols_num:
            df_tol[col] = pd.to_numeric(df_tol[col].astype(str).str.replace(',', ''), errors='coerce').fillna(0)

        col_sec = obtener_columna(df_tol, ['SECCIÓN', 'SECCION'])
        if col_sec:
            df_tol.rename(columns={col_sec: 'SECCION'}, inplace=True)
            
        df_tol['VOTOS_TOTALES'] = df_tol[obtener_columna(df_tol, ['TOTAL_VOTOS', 'TOTAL', 'TOTAL VOTOS'])]
        df_tol['LISTA_NOMINAL_HIST'] = df_tol[obtener_columna(df_tol, ['LISTA_NOMINAL', 'LISTA NOMINAL'])]
        
        # Bloques de Estructura Dinámica
        if anio == 2017:
            columnas_tradicional = ['PRI', 'PVEM', 'NVA_ALIANZA', 'ES', 'PRI_PVEM_NVA_ALIANZA_ES', 'PRI_PVEM_NVA_ALIANZA', 'PRI_PVEM_ES', 'PRI_NVA_ALIANZA_ES', 'PRI_PVEM', 'PRI_NVA_ALIANZA', 'PRI_ES', 'PVEM_NVA_ALIANZA_ES', 'PVEM_NVA_ALIANZA', 'PVEM_ES', 'NVA_ALIANZA_ES']
            df_tol['ESTRUCTURA_TRADICIONAL'] = df_tol[[c for c in columnas_tradicional if c in df_tol.columns]].sum(axis=1)
            df_tol['ESTRUCTURA_IZQUIERDA'] = df_tol.get('MORENA', 0) + df_tol.get('PT', 0)
        else:
            columnas_tradicional = ['PAN', 'PRI', 'PRD', 'NAEM', 'PAN_PRI_PRD_NAEM', 'PAN_PRI_PRD', 'PAN_PRI_NAEM', 'PAN_PRD_NAEM', 'PRI_PRD_NAEM', 'PAN_PRI', 'PAN_PRD', 'PAN_NAEM', 'PRI_PRD', 'PRI_NAEM', 'PRD_NAEM']
            df_tol['ESTRUCTURA_TRADICIONAL'] = df_tol[[c for c in columnas_tradicional if c in df_tol.columns]].sum(axis=1)
            df_tol['ESTRUCTURA_IZQUIERDA'] = df_tol.get('PVEM_PT_MORENA', 0)

        df_estrategico = df_tol[['SECCION', 'ESTRUCTURA_TRADICIONAL', 'ESTRUCTURA_IZQUIERDA', 'VOTOS_TOTALES', 'LISTA_NOMINAL_HIST']].copy()
        
        # Inyección de Volatilidad
        col_nulos = obtener_columna(df_tol, ['NULOS', 'VOTOS_NULOS'])
        df_estrategico['VOTOS_NULOS'] = df_tol[col_nulos] if col_nulos else 0
        df_estrategico['ABSTENCIONISMO_HIST'] = (df_estrategico['LISTA_NOMINAL_HIST'] - df_estrategico['VOTOS_TOTALES']).clip(lower=0)

        df_estrategico['ANIO'] = anio
        df_estrategico['TIPO_ELECCION'] = 'GUBERNATURA'
        dataframes_limpios.append(df_estrategico)

    if dataframes_limpios:
        df_estatal_historico = pd.concat(dataframes_limpios, ignore_index=True)
        # Agrupar casillas a nivel sección
        df_estatal_historico = df_estatal_historico.groupby(['SECCION', 'ANIO', 'TIPO_ELECCION']).sum().reset_index()
        
        ruta_salida = os.path.join(INTERIM_DIR, 'TOLUCA_ESTATAL_LIMPIO.csv')
        df_estatal_historico.to_csv(ruta_salida, index=False)
        print(f"  ✅ Completado: Guardado en {ruta_salida}")
    else:
        print("  ❌ Error: No se pudo procesar ningún archivo estatal.")

# ==============================================================================
# EJECUCIÓN PRINCIPAL DEL PIPELINE
# ==============================================================================
if __name__ == '__main__':
    print("==================================================")
    print(" INICIANDO ETL: SISTEMA DE INTELIGENCIA SITS")
    print("==================================================")
    
    # 1. Procesar la meta futura
    procesar_padron_2026()
    
    # 2. Procesar comportamiento histórico
    limpiar_datos_municipales()
    limpiar_datos_estatales()
    
    print("\n==================================================")
    print(" FASE 1 (EXTRACCIÓN Y LIMPIEZA) FINALIZADA 🚀")
    print("==================================================")