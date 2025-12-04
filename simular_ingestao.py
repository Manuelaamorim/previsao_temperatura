import pandas as pd
import requests
import time
import os
from datetime import datetime
import numpy as np
import logging 
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

FASTAPI_URL = "http://localhost:8000/ingest"
CSV_PATH = "notebooks/data/generatedBy_react-csv.csv"
STATION_CODE = "A301"
REQUEST_TIMEOUT = 60 

def fix_hora_inmet_to_hour(h):
    h_str = str(h).zfill(4)
    try:
        hour = int(h_str[:2])
        if 0 <= hour <= 23:
            return hour
        return np.nan
    except:
        return np.nan

def clean_and_format_dataframe(df_raw):
    logger.info("Iniciando limpeza e formatação dos dados...") 
    df = df_raw.copy()
    column_mapping = {
        'Temp. Ins. (C)': 'temperature',
        'Umi. Ins. (%)': 'humidity',
        'Pressao Ins. (hPa)': 'pressure',
        'Vel. Vento (m/s)': 'wind_speed',
        'Dir. Vento (m/s)': 'wind_direction', 
        'Radiacao (KJ/m²)': 'radiation',
        'Chuva (mm)': 'precipitation',
        'Hora (UTC)': 'Hora_UTC'
    }
    df.rename(columns=column_mapping, inplace=True)
    cols_to_numeric = list(column_mapping.values())
    
    for col in cols_to_numeric:
        if col in df.columns and col != 'Hora_UTC':
            df[col] = pd.to_numeric(
                df[col].astype(str).str.replace(",", "."), 
                errors="coerce"
            )

    df = df.replace([-9999, -9999.0], np.nan)
    df['HoraCorrigida'] = df['Hora_UTC'].apply(fix_hora_inmet_to_hour)
    df["datetime_full"] = pd.to_datetime(
        df["Data"] + " " + df["HoraCorrigida"].astype(str).str.zfill(2) + ":00",
        dayfirst=True,
        errors='coerce'
    )
    
    df = df.set_index("datetime_full").sort_index()
    final_cols = ['temperature', 'humidity', 'pressure', 'wind_speed', 'wind_direction', 'radiation', 'precipitation']
    
    df_clean = df[final_cols]
    df_clean = df_clean.resample("H").mean()
    df_clean = df_clean.interpolate(method="time")
    df_clean = df_clean.ffill().bfill() 
    df_clean.dropna(inplace=True)
    
    return df_clean

def simulate_ingestion():
    if not os.path.exists(CSV_PATH):
        print(f"ERRO: Arquivo CSV não encontrado no caminho: {CSV_PATH}")
        print("Certifique-se de que o CSV está na pasta 'data/'")
        return

    try:
        df_raw = pd.read_csv(CSV_PATH, sep=";")
    except Exception as e:
        print(f"ERRO ao ler o CSV: {e}")
        return

    df_clean = clean_and_format_dataframe(df_raw)
    
    print(f"\nTotal de {len(df_clean)} registros limpos e prontos para ingestão.")
    
    success_count = 0
    start_time = time.time()

    for index, row in df_clean.iterrows():
        payload = {
            "station_code": STATION_CODE,
            "temperature": round(row["temperature"], 2),
            "humidity": round(row["humidity"], 2),
            "pressure": round(row["pressure"], 2),
            "wind_speed": round(row["wind_speed"], 2),
            "wind_direction": round(row["wind_direction"], 2),
            "radiation": round(row["radiation"], 2),
            "precipitation": round(row["precipitation"], 2),
        }

        try:
            response = requests.post(FASTAPI_URL, json=payload, timeout=REQUEST_TIMEOUT)
            
            if response.status_code == 200:
                success_count += 1
                if success_count % 100 == 0:
                     print(f"Processado {success_count}/{len(df_clean)} registros com sucesso.")
            else:
                print(f"Falha na ingestão do registro em {index}. Status: {response.status_code}, Detalhe: {response.text}")
        
        except requests.exceptions.ConnectionError:
            print(f"\nERRO: Não foi possível conectar ao FastAPI em {FASTAPI_URL}.")
            print("Certifique-se de que o serviço 'fastapi' está rodando (docker-compose ps).")
            break
        except requests.exceptions.ReadTimeout:
             print(f"\nERRO: Timeout (>{REQUEST_TIMEOUT}s) ao processar o registro em {index}. Tente aumentar o REQUEST_TIMEOUT.")
             break
        except Exception as e:
            print(f"Erro inesperado no registro {index}: {e}")
            break

    end_time = time.time()
    
    print("\n--- RESUMO DA INGESTÃO ---")
    print(f"Registros Enviados: {len(df_clean)}")
    print(f"Registros Salvos (DB/S3): {success_count}")
    print(f"Tempo Total: {end_time - start_time:.2f} segundos")
    
    if success_count == len(df_clean):
        print("🎉 SUCESSO! O banco de dados foi preenchido com a totalidade dos dados.")
    else:
        print("⚠️ Atenção: Houve falhas na ingestão.")

if __name__ == "__main__":
    simulate_ingestion()