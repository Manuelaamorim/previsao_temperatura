import pandas as pd
import requests
import time
import math

# --- CONFIGURAÇÃO ---
API_URL = "http://localhost:8000/ingest"
# Caminho do seu arquivo (verifique se está na pasta correta)
CSV_FILE = "notebooks/data/generatedBy_react-csv.csv" 
DELAY_SEGUNDOS = 0.5  # Rápido para encher o banco logo

def limpar_valor(valor):
    """Converte '25,4' para 25.4 e trata valores vazios"""
    if pd.isna(valor) or valor == "":
        return 0.0
    if isinstance(valor, str):
        # Troca vírgula por ponto para o Python entender
        valor = valor.replace(',', '.')
    try:
        return float(valor)
    except ValueError:
        return 0.0

def enviar_dados(payload):
    try:
        response = requests.post(API_URL, json=payload)
        if response.status_code == 200:
            print(f"✅ [OK] Data: {payload['station_code']} | Temp: {payload['temperature']}°C")
        else:
            print(f"❌ [ERRO API] {response.status_code}: {response.text}")
    except Exception as e:
        print(f"❌ [ERRO CONEXÃO] {e}")

print("--- INICIANDO INGESTÃO DO CSV INMET ---")

try:
    # Lê o CSV com separador de ponto e vírgula (padrão INMET)
    df = pd.read_csv(CSV_FILE, sep=";", encoding="utf-8") # Se der erro de encoding, tente 'latin1'
    
    print(f"Arquivo carregado! Total de linhas: {len(df)}")
    print("Iniciando envio...")

    for index, row in df.iterrows():
        # Mapeamento: Coluna do CSV -> Campo da API
        # Usamos 'Temp. Ins. (C)' (instantânea) como temperatura principal
        payload = {
            "station_code": "A301", # Ou use uma coluna se tiver
            "temperature": limpar_valor(row.get("Temp. Ins. (C)")),
            "humidity": limpar_valor(row.get("Umi. Ins. (%)")),
            "pressure": limpar_valor(row.get("Pressao Ins. (hPa)")),
            "wind_speed": limpar_valor(row.get("Vel. Vento (m/s)")),
            "wind_direction": limpar_valor(row.get("Dir. Vento (m/s)")),
            "radiation": limpar_valor(row.get("Radiacao (KJ/m²)")),
            "precipitation": limpar_valor(row.get("Chuva (mm)"))
        }
        
        # Envia e espera um pouquinho
        enviar_dados(payload)
        time.sleep(DELAY_SEGUNDOS)

except FileNotFoundError:
    print(f"❌ Erro: O arquivo '{CSV_FILE}' não foi encontrado.")
    print("Verifique se o arquivo está dentro da pasta 'notebooks/data' e se o nome está correto.")
except Exception as e:
    print(f"❌ Erro inesperado: {e}")