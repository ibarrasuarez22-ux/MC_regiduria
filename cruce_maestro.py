import pandas as pd
import numpy as np
import os
import re
import warnings

warnings.filterwarnings('ignore')

# ==============================================================================
# CONFIGURACIÓN DE RUTAS 
# ==============================================================================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
INTERIM_DIR = os.path.join(BASE_DIR, 'data', '02_interim')
PROCESSED_DIR = os.path.join(BASE_DIR, 'data', '03_processed')
RAW_CENSO_DIR = os.path.join(BASE_DIR, 'data', '01_raw', 'censo_marginacion')

os.makedirs(PROCESSED_DIR, exist_ok=True)
os.makedirs(RAW_CENSO_DIR, exist_ok=True) 

ruta_mun = os.path.join(INTERIM_DIR, 'TOLUCA_MUNICIPAL_LIMPIO.csv')
ruta_est = os.path.join(INTERIM_DIR, 'TOLUCA_ESTATAL_LIMPIO.csv')
ruta_padron = os.path.join(INTERIM_DIR, 'TOLUCA_PADRON_2026_LIMPIO.csv')

# ==============================================================================
# APLANADORA UNIVERSAL REGEX
# ==============================================================================
def aplanar_columnas(columnas, sufijo=""):
    nuevas_cols = []
    for col in columnas:
        if isinstance(col, tuple):
            nombre = "_".join([str(x) for x in col if pd.notna(x) and str(x).strip() != ''])
        else:
            nombre = str(col)
        nombre = re.sub(r'\.0$', '', nombre)
        nombre = nombre.strip() + sufijo
        nuevas_cols.append(nombre)
    return nuevas_cols

# ==============================================================================
# MOTOR PRINCIPAL
# ==============================================================================
def generar_cruce_maestro():
    print("==================================================")
    print(" INICIANDO MOTOR SITS: CRUCE MAESTRO PREDICTIVO")
    print("==================================================")
    
    if not os.path.exists(ruta_padron):
        print("  ❌ Error: No existe el Padrón 2026 limpio. Ejecuta Fase 1 primero.")
        return
        
    print("  📁 Cargando Padrón 2026...")
    df_maestro = pd.read_csv(ruta_padron)
    df_maestro['SECCION'] = pd.to_numeric(df_maestro['SECCION'], errors='coerce').fillna(0).astype(int)
    
    # ------------------------------------------------------------------
    # PIVOT MUNICIPAL
    # ------------------------------------------------------------------
    if os.path.exists(ruta_mun):
        print("  📊 Fusionando Históricos Municipales...")
        df_mun = pd.read_csv(ruta_mun)
        df_mun['SECCION'] = pd.to_numeric(df_mun['SECCION'], errors='coerce').fillna(0).astype(int)
        
        df_mun_pivot = df_mun.pivot_table(
            index='SECCION', 
            columns='ANIO', 
            values=['ESTRUCTURA_TRADICIONAL', 'ESTRUCTURA_IZQUIERDA', 'VOTOS_MC', 'VOTOS_NULOS', 'VOTOS_TOTALES', 'PARTICIPACION_PCT', 'LISTA_NOMINAL_HIST'],
            aggfunc='sum'
        ).fillna(0)
        
        df_mun_pivot.columns = aplanar_columnas(df_mun_pivot.columns)
        df_maestro = pd.merge(df_maestro, df_mun_pivot.reset_index(), on='SECCION', how='left').fillna(0)
    
    # ------------------------------------------------------------------
    # PIVOT ESTATAL
    # ------------------------------------------------------------------
    if os.path.exists(ruta_est):
        print("  📊 Fusionando Históricos Estatales...")
        df_est = pd.read_csv(ruta_est)
        df_est['SECCION'] = pd.to_numeric(df_est['SECCION'], errors='coerce').fillna(0).astype(int)
        
        df_est_pivot = df_est.pivot_table(
            index='SECCION',
            columns='ANIO',
            values=['ESTRUCTURA_TRADICIONAL', 'ESTRUCTURA_IZQUIERDA', 'VOTOS_TOTALES', 'LISTA_NOMINAL_HIST'],
            aggfunc='sum'
        ).fillna(0)
        
        df_est_pivot.columns = aplanar_columnas(df_est_pivot.columns, sufijo="_EST")
        df_maestro = pd.merge(df_maestro, df_est_pivot.reset_index(), on='SECCION', how='left').fillna(0)

    # ------------------------------------------------------------------
    # CENSO JÓVENES (EXTRACCIÓN QUIRÚRGICA)
    # ------------------------------------------------------------------
    print("  🧑‍🤝‍🧑 Buscando Censo Demográfico 2020...")
    archivos_censo = [f for f in os.listdir(RAW_CENSO_DIR) if 'INE_SECCION' in f.upper() and f.endswith('.csv')]
    
    if len(archivos_censo) > 0:
        archivo_objetivo = archivos_censo
        if isinstance(archivo_objetivo, list): 
            archivo_objetivo = archivo_objetivo
        archivo_objetivo = str(archivo_objetivo).replace("['", "").replace("']", "").strip()
        
        ruta_eceg = os.path.join(RAW_CENSO_DIR, archivo_objetivo)
        print(f"  [Info] Archivo localizado: {archivo_objetivo}. Integrando Jóvenes...")
        
        df_censo = pd.read_csv(ruta_eceg, low_memory=False)
        # Forzar un nombre de columnas en texto plano sin complicaciones
        df_censo.columns = [str(c).strip().upper() for c in df_censo.columns]
        
        col_sec = [c for c in df_censo.columns if c in ['SECCION', 'SECCIÓN', 'SEC']]
        if col_sec:
            c_s = col_sec
            
            # 💥 EXTRACCIÓN QUIRÚRGICA: Pluck de las series seguras sin alterar la tabla corrupta
            serie_sec = df_censo[c_s]
            if isinstance(serie_sec, pd.DataFrame): serie_sec = serie_sec.iloc[:, 0]
                
            serie_p15 = df_censo['P_15A17'] if 'P_15A17' in df_censo.columns else pd.Series(0, index=df_censo.index)
            if isinstance(serie_p15, pd.DataFrame): serie_p15 = serie_p15.iloc[:, 0]
                
            serie_p12 = df_censo['P_12A14'] if 'P_12A14' in df_censo.columns else pd.Series(0, index=df_censo.index)
            if isinstance(serie_p12, pd.DataFrame): serie_p12 = serie_p12.iloc[:, 0]
            
            # Creamos una tabla virgen 100% limpia
            df_jovenes = pd.DataFrame()
            df_jovenes['SECCION'] = pd.to_numeric(serie_sec, errors='coerce')
            df_jovenes['P_15A17'] = pd.to_numeric(serie_p15, errors='coerce').fillna(0)
            df_jovenes['P_12A14'] = pd.to_numeric(serie_p12, errors='coerce').fillna(0)
            
            df_jovenes = df_jovenes.dropna(subset=['SECCION'])
            df_jovenes['SECCION'] = df_jovenes['SECCION'].astype(int)
            
            df_jovenes['ELECTORES_MENORES_25'] = np.floor((df_jovenes['P_15A17'] + df_jovenes['P_12A14']) * 0.96).astype(int)
            
            df_censo_agrupado = df_jovenes.groupby('SECCION')['ELECTORES_MENORES_25'].sum().reset_index()
            df_maestro = pd.merge(df_maestro, df_censo_agrupado, on='SECCION', how='left').fillna(0)
    else:
        print("  ⚠️ Advertencia: No se encontró ningún archivo 'INE_SECCION' en la carpeta censo_marginacion. Jóvenes en 0.")
        df_maestro['ELECTORES_MENORES_25'] = 0

    # ------------------------------------------------------------------
    # MOTOR DE MÉTRICAS BASE
    # ------------------------------------------------------------------
    print("  🧠 Calculando Métricas Base SITS...")
    
    def get_safe_series(col_name):
        if col_name in df_maestro.columns:
            return df_maestro[col_name].fillna(0)
        else:
            return pd.Series(0, index=df_maestro.index)

    cols_trad = [c for c in df_maestro.columns if 'ESTRUCTURA_TRADICIONAL' in c]
    cols_izq = [c for c in df_maestro.columns if 'ESTRUCTURA_IZQUIERDA' in c]
    
    df_maestro['VOTO_DURO_TRADICIONAL'] = df_maestro[cols_trad].mean(axis=1).fillna(0) if cols_trad else 0
    df_maestro['VOTO_DURO_IZQUIERDA'] = df_maestro[cols_izq].mean(axis=1).fillna(0) if cols_izq else 0
    
    v_24 = get_safe_series('VOTOS_TOTALES_2024')
    v_21 = get_safe_series('VOTOS_TOTALES_2021')
    v_23_est = get_safe_series('VOTOS_TOTALES_2023_EST')
    
    df_maestro['ORFANDAD_NARANJA'] = (v_24 - v_23_est).clip(lower=0) 
    
    nulos_24 = get_safe_series('VOTOS_NULOS_2024')
    max_nulos = nulos_24.max()
    
    if pd.isna(max_nulos) or max_nulos <= 0:
        df_maestro['VOLATILIDAD_PSI'] = 0.0
    else:
        df_maestro['VOLATILIDAD_PSI'] = (nulos_24 / max_nulos).fillna(0)

    lista_hist = get_safe_series('LISTA_NOMINAL_HIST_2024')
    if lista_hist.sum() == 0: 
        lista_hist = get_safe_series('LISTA_NOMINAL_2026')
        
    df_maestro['ABSTENCION_PROYECTADA_2026'] = (lista_hist - v_24).clip(lower=0)

    # ------------------------------------------------------------------
    # MOTOR DE ÍNDICES ESTRATÉGICOS
    # ------------------------------------------------------------------
    print("  🚀 Inyectando Índices Estratégicos (IEE e IRC)...")
    
    lista_nom_segura = df_maestro.get('LISTA_NOMINAL_2026', pd.Series(1, index=df_maestro.index)).fillna(1).replace(0, 1) 
    
    df_maestro['INDICE_IRC'] = (((df_maestro['VOTO_DURO_TRADICIONAL'] + df_maestro['VOTO_DURO_IZQUIERDA']) / lista_nom_segura) * 100).round(2)
    df_maestro['INDICE_IRC'] = df_maestro['INDICE_IRC'].fillna(0).clip(lower=0, upper=100)
    
    v_tot_24_safe = v_24.replace(0, 1) 
    df_maestro['INDICE_IEE'] = (((df_maestro['ORFANDAD_NARANJA'] / v_tot_24_safe) + df_maestro['VOLATILIDAD_PSI']) * 50).round(2)
    df_maestro['INDICE_IEE'] = df_maestro['INDICE_IEE'].fillna(0).clip(lower=0, upper=100)
    
    mc_24 = get_safe_series('VOTOS_MC_2024')
    df_maestro['VOTOS_TOTALES_PROYECTADOS_2026'] = (mc_24 * (1 + (df_maestro['INDICE_IEE']/100))).fillna(0).astype(int)

    # ------------------------------------------------------------------
    # EXPORTACIÓN
    # ------------------------------------------------------------------
    ruta_salida = os.path.join(PROCESSED_DIR, 'CRUCE_MAESTRO_INDICADORES.csv')
    df_maestro.to_csv(ruta_salida, index=False)
    
    print(f"  ✅ Cruce Maestro Finalizado: {len(df_maestro)} secciones procesadas.")
    print(f"  📁 Base Demográfica y Electoral guardada en: {ruta_salida}")

if __name__ == '__main__':
    generar_cruce_maestro()