import pandas as pd
import numpy as np
import os

# ==============================================================================
# CONFIGURACIÓN DE RUTAS (ARQUITECTURA SITS)
# ==============================================================================
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
INTERIM_DIR = os.path.join(BASE_DIR, 'data', '02_interim')
PROCESSED_DIR = os.path.join(BASE_DIR, 'data', '03_processed')
RAW_CENSO_DIR = os.path.join(BASE_DIR, 'data', '01_raw', 'censo_marginacion')

os.makedirs(PROCESSED_DIR, exist_ok=True)

ruta_mun = os.path.join(INTERIM_DIR, 'TOLUCA_MUNICIPAL_LIMPIO.csv')
ruta_est = os.path.join(INTERIM_DIR, 'TOLUCA_ESTATAL_LIMPIO.csv')
ruta_padron_2026 = os.path.join(INTERIM_DIR, 'TOLUCA_PADRON_2026_LIMPIO.csv')
ruta_eceg = os.path.join(RAW_CENSO_DIR, 'INE_SECCION_2020.csv')

def generar_cruce_maestro():
    print("==================================================")
    print(" INICIANDO MOTOR SITS: CRUCE MAESTRO PREDICTIVO")
    print("==================================================")
    
    if not os.path.exists(ruta_mun) or not os.path.exists(ruta_est):
        print("  ❌ Error: Faltan archivos históricos.")
        return
        
    df_mun = pd.read_csv(ruta_mun)
    df_est = pd.read_csv(ruta_est)
    
    # 1. FILTRO ANTIVIRUS: Eliminar filas basura
    df_mun = df_mun[pd.to_numeric(df_mun['SECCION'], errors='coerce').notnull()].copy()
    df_est = df_est[pd.to_numeric(df_est['SECCION'], errors='coerce').notnull()].copy()
    
    df_mun['SECCION'] = df_mun['SECCION'].astype(str).str.replace(r'\.0$', '', regex=True).str.strip()
    df_est['SECCION'] = df_est['SECCION'].astype(str).str.replace(r'\.0$', '', regex=True).str.strip()
    
    df_2024 = df_mun[df_mun['ANIO'] == 2024].copy().rename(columns={
        'VOTOS_MC': 'VOTOS_MC_24', 'VOTOS_TOTALES': 'VOTOS_TOTALES_24', 
        'LISTA_NOMINAL_HIST': 'LISTA_NOMINAL_24', 'VOTOS_NULOS': 'VOTOS_NULOS_24'
    })
    df_2021 = df_mun[df_mun['ANIO'] == 2021].copy().rename(columns={'VOTOS_MC': 'VOTOS_MC_21'})
    
    maestro = pd.merge(df_2024[['SECCION', 'VOTOS_MC_24', 'VOTOS_TOTALES_24', 'LISTA_NOMINAL_24', 'VOTOS_NULOS_24']], 
                       df_2021[['SECCION', 'VOTOS_MC_21']], on='SECCION', how='outer')
                       
    # 2. INYECCIÓN DEL HORIZONTE 2026 (INCLUYENDO GÉNERO)
    if os.path.exists(ruta_padron_2026):
        df_padron = pd.read_csv(ruta_padron_2026)
        df_padron = df_padron[pd.to_numeric(df_padron['SECCION'], errors='coerce').notnull()].copy()
        df_padron['SECCION'] = df_padron['SECCION'].astype(str).str.replace(r'\.0$', '', regex=True).str.strip()
        
        columnas_padron = ['SECCION', 'LISTA_NOMINAL_2026']
        if 'HOMBRES_2026' in df_padron.columns: columnas_padron.append('HOMBRES_2026')
        if 'MUJERES_2026' in df_padron.columns: columnas_padron.append('MUJERES_2026')
        
        maestro = pd.merge(maestro, df_padron[columnas_padron], on='SECCION', how='left')
    else:
        maestro['LISTA_NOMINAL_2026'] = maestro['LISTA_NOMINAL_24']

    # --- BLINDAJE CONTRA EL ERROR DE NaN (IntCastingNaNError) ---
    # Si la lista nominal 26 o 24 está vacía, forzamos un CERO absoluto
    maestro['LISTA_NOMINAL_2026'] = pd.to_numeric(maestro['LISTA_NOMINAL_2026'], errors='coerce').fillna(maestro['LISTA_NOMINAL_24']).fillna(0)

    # PARACAÍDAS DEMOGRÁFICO DE GÉNERO
    if 'HOMBRES_2026' not in maestro.columns: maestro['HOMBRES_2026'] = 0
    if 'MUJERES_2026' not in maestro.columns: maestro['MUJERES_2026'] = 0
    
    maestro['HOMBRES_2026'] = pd.to_numeric(maestro['HOMBRES_2026'], errors='coerce').fillna(0)
    maestro['MUJERES_2026'] = pd.to_numeric(maestro['MUJERES_2026'], errors='coerce').fillna(0)

    maestro['HOMBRES_2026'] = np.where(maestro['HOMBRES_2026'] <= 0, np.floor(maestro['LISTA_NOMINAL_2026'] * 0.48).astype(int), maestro['HOMBRES_2026'])
    maestro['MUJERES_2026'] = np.where(maestro['MUJERES_2026'] <= 0, np.floor(maestro['LISTA_NOMINAL_2026'] * 0.52).astype(int), maestro['MUJERES_2026'])


    # 3. INYECCIÓN DE CENSO INEGI (JUVENTUD)
    if os.path.exists(ruta_eceg):
        df_censo = pd.read_csv(ruta_eceg, low_memory=False)
        # Normalizar para prevenir fallos por formato del INEGI
        df_censo.columns = df_censo.columns.astype(str).str.upper().str.strip()
        
        col_sec = [c for c in df_censo.columns if c in ['SECCION', 'SECCIÓN', 'SEC']]
        
        if col_sec:
            df_censo = df_censo[pd.to_numeric(df_censo[col_sec[0]], errors='coerce').notnull()].copy()
            df_censo['SECCION'] = df_censo[col_sec[0]].astype(str).str.replace(r'\.0$', '', regex=True).str.strip()
            
            for col in ['P_18YMAS', 'P_12A14', 'P_15A17', 'POBTOT']:
                if col in df_censo.columns:
                    df_censo[col] = pd.to_numeric(df_censo[col].astype(str).str.replace(',', ''), errors='coerce').fillna(0)
                    
            if 'P_12A14' in df_censo.columns and 'P_15A17' in df_censo.columns:
                df_censo['ELECTORES_MENORES_25'] = np.floor((df_censo['P_12A14'] + df_censo['P_15A17']) * 0.96).astype(int)
            else:
                df_censo['ELECTORES_MENORES_25'] = 0
                
            maestro = pd.merge(maestro, df_censo[['SECCION', 'ELECTORES_MENORES_25']], on='SECCION', how='left')
        else:
             maestro['ELECTORES_MENORES_25'] = 0
    else:
        maestro['ELECTORES_MENORES_25'] = 0
        
    # PARACAÍDAS PREDICTIVO DE JÓVENES (16.5%)
    maestro['ELECTORES_MENORES_25'] = maestro['ELECTORES_MENORES_25'].fillna(0)
    maestro['ELECTORES_MENORES_25'] = np.where(maestro['ELECTORES_MENORES_25'] <= 0, np.floor(maestro['LISTA_NOMINAL_2026'] * 0.165).astype(int), maestro['ELECTORES_MENORES_25'])

    # 4. MATEMÁTICAS ESTRICTAS SITS
    maestro['VOTOS_TOTALES_24'] = pd.to_numeric(maestro['VOTOS_TOTALES_24'], errors='coerce').fillna(0)
    
    maestro['BASELINE_MC'] = maestro['VOTOS_MC_24'].fillna(0)
    maestro['ABSTENCION_PROYECTADA_2026'] = (maestro['LISTA_NOMINAL_2026'] - maestro['VOTOS_TOTALES_24']).clip(lower=0)
    maestro['CRECIMIENTO_MC_ABS'] = maestro['VOTOS_MC_24'].fillna(0) - maestro['VOTOS_MC_21'].fillna(0)
    
    # Maquinarias Estatales
    est_resumen = df_est.groupby('SECCION').agg(
        VOTO_DURO_TRADICIONAL=('ESTRUCTURA_TRADICIONAL', 'mean'),
        VOTO_DURO_IZQUIERDA=('ESTRUCTURA_IZQUIERDA', 'mean'),
        VOTOS_TOTALES_23=('VOTOS_TOTALES', 'max')
    ).reset_index()
    maestro = pd.merge(maestro, est_resumen, on='SECCION', how='left')
    
    maestro['ORFANDAD_NARANJA'] = (maestro['VOTOS_TOTALES_24'].fillna(0) - maestro['VOTOS_TOTALES_23'].fillna(0)).clip(lower=0)
    
    # --- BLOQUE BLINDADO CONTRA ERRORES DE DIVISIÓN POR CERO ---
    max_nulos = maestro['VOTOS_NULOS_24'].max()
    if pd.isna(max_nulos) or max_nulos <= 0:
        maestro['VOLATILIDAD_PSI'] = 0.0
    else:
        maestro['VOLATILIDAD_PSI'] = (maestro['VOTOS_NULOS_24'] / max_nulos).fillna(0)
    
    max_abs = maestro['ABSTENCION_PROYECTADA_2026'].max()
    if pd.isna(max_abs) or max_abs <= 0:
        score_abs = 0.0
    else:
        score_abs = (maestro['ABSTENCION_PROYECTADA_2026'] / max_abs).fillna(0)
    
    maestro['INDICE_PRIORIDAD_SITS'] = ((score_abs * 0.5) + (maestro['VOLATILIDAD_PSI'] * 0.5)) * 100
    maestro['INDICE_PRIORIDAD_SITS'] = maestro['INDICE_PRIORIDAD_SITS'].round(2).fillna(0)
    
    # Limpiar posibles NaN para exportación
    maestro = maestro.fillna(0)
    
    ruta_salida = os.path.join(PROCESSED_DIR, 'CRUCE_MAESTRO_INDICADORES.csv')
    maestro.to_csv(ruta_salida, index=False)
    print(f"  ✅ Cruce Maestro SITS generado con éxito. Sin filas basura. Motor Juvenil Activado.")

if __name__ == '__main__':
    generar_cruce_maestro()