import pandas as pd
import requests
import time
import os
from datetime import datetime
import numpy as np
import logging # Importa o módulo logging

# --- CONFIGURAÇÃO DE LOGGING ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
# --- FIM DA CONFIGURAÇÃO DE LOGGING ---

# --- CONFIGURAÇÃO ---
# O endpoint do seu FastAPI rodando no Docker (porta 8000 mapeada)
FASTAPI_URL = "http://localhost:8000/ingest"
# O nome do seu arquivo CSV original
CSV_PATH = "notebooks/data/generatedBy_react-csv.csv"
# O código da estação (assumindo que o CSV é de uma única estação,
# caso contrário, você precisaria extrair isso do arquivo)
STATION_CODE = "A301" 
# Aumentamos o timeout para 60 segundos (1 minuto) para garantir 
# que o FastAPI tenha tempo de processar a requisição lenta
REQUEST_TIMEOUT = 60 

def fix_hora_inmet_to_hour(h):
    """
    Converte hora INMET (ex: 100 -> 1, 1300 -> 13) para inteiro (hora).
    """
    h_str = str(h).zfill(4)
    # Pega os primeiros dois dígitos como a hora
    try:
        hour = int(h_str[:2])
        if 0 <= hour <= 23:
            return hour
        return np.nan
    except:
        return np.nan

def clean_and_format_dataframe(df_raw):
    """
    Realiza a limpeza e ajuste de tipos para corresponder ao formato do FastAPI.
    """
    # Linha que causava o erro NameError
    logger.info("Iniciando limpeza e formatação dos dados...") 
    
    # 1. Ajustar nomes das colunas e remover ','
    df = df_raw.copy()
    
    # Mapeamento do CSV bruto para o modelo WeatherInput do FastAPI
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
    
    # Garante que as colunas essenciais existam
    df.rename(columns=column_mapping, inplace=True)

    # 2. Converte colunas numéricas (incluindo substituição de vírgula)
    cols_to_numeric = list(column_mapping.values())
    
    for col in cols_to_numeric:
        if col in df.columns and col != 'Hora_UTC':
            df[col] = pd.to_numeric(
                df[col].astype(str).str.replace(",", "."), 
                errors="coerce"
            )

    # 3. Trata valores nulos/inválidos (-9999) para NaN
    df = df.replace([-9999, -9999.0], np.nan)
    
    # 4. Cria o campo datetime para fins de ordenação
    df['HoraCorrigida'] = df['Hora_UTC'].apply(fix_hora_inmet_to_hour)
    
    # Juntar Data e Hora corrigida
    df["datetime_full"] = pd.to_datetime(
        df["Data"] + " " + df["HoraCorrigida"].astype(str).str.zfill(2) + ":00",
        dayfirst=True,
        errors='coerce'
    )
    
    df = df.set_index("datetime_full").sort_index()

    # 5. Seleciona e renomeia as colunas finais
    final_cols = ['temperature', 'humidity', 'pressure', 'wind_speed', 'wind_direction', 'radiation', 'precipitation']
    
    df_clean = df[final_cols]
    
    # Preenche NaN com interpolação, como você fez no notebook de EDA
    df_clean = df_clean.resample("H").mean()
    df_clean = df_clean.interpolate(method="time")
    df_clean = df_clean.ffill().bfill() 
    
    # Remove linhas onde a interpolação falhou (casos raros)
    df_clean.dropna(inplace=True)
    
    return df_clean

def simulate_ingestion():
    """
    Lê o CSV, formata os dados e envia para o endpoint do FastAPI.
    """
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
        # Converte o timestamp para o formato ISO 8601 (embora o FastAPI use datetime.utcnow, é bom incluir)
        # O FastAPI está esperando apenas os 7 campos (temperature, humidity, etc.)
        
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
            # ALTERADO: Aumentando o timeout para 60 segundos
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
    # Garante que o uvicorn do FastAPI está rodando ANTES de executar este script
    # E que o PostgreSQL está UP.
    simulate_ingestion()