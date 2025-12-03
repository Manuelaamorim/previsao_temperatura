import pandas as pd
import requests
import time
import math

# --- CONFIGURAÇÃO ---
# 1. Configuração da API (Para salvar no Banco)
API_URL = "http://localhost:8000/ingest"

# 2. Configuração do ThingsBoard (Para visualizar o Gráfico)
TB_HOST = "http://localhost:9090"
TB_TOKEN = 'zb4uP0BTV7zGsvPnu9IG'  # <--- Seu token já está aqui
TB_URL = f"{TB_HOST}/api/v1/{TB_TOKEN}/telemetry"

# Arquivo de dados
CSV_FILE = "notebooks/data/generatedBy_react-csv.csv" 
DELAY_SEGUNDOS = 0.5 

def limpar_valor(valor):
    """Converte '25,4' para 25.4 e trata valores vazios"""
    if pd.isna(valor) or valor == "":
        return 0.0
    if isinstance(valor, str):
        valor = valor.replace(',', '.')
    try:
        return float(valor)
    except ValueError:
        return 0.0

def enviar_para_api(payload):
    """Envia para o seu Banco de Dados (Via FastAPI)"""
    try:
        response = requests.post(API_URL, json=payload)
        return response.status_code == 200
    except:
        return False

def enviar_para_thingsboard(payload):
    """Envia para o ThingsBoard (Visualização)"""
    try:
        # ThingsBoard aceita o JSON direto
        response = requests.post(TB_URL, json=payload)
        return response.status_code == 200
    except:
        return False

print("--- INICIANDO SIMULADOR HÍBRIDO (BANCO + DASHBOARD) ---")
print(f"Alvo API: {API_URL}")
print(f"Alvo ThingsBoard: {TB_URL}")

try:
    # Tenta ler o CSV
    df = pd.read_csv(CSV_FILE, sep=";", encoding="utf-8")
    
    print(f"Arquivo carregado! Total de linhas: {len(df)}")
    print("Iniciando envio...")

    for index, row in df.iterrows():
        # Prepara o pacote de dados
        payload = {
            "station_code": "A301", 
            "temperature": limpar_valor(row.get("Temp. Ins. (C)")),
            "humidity": limpar_valor(row.get("Umi. Ins. (%)")),
            "pressure": limpar_valor(row.get("Pressao Ins. (hPa)")),
            "wind_speed": limpar_valor(row.get("Vel. Vento (m/s)")),
            "wind_direction": limpar_valor(row.get("Dir. Vento (m/s)")),
            "radiation": limpar_valor(row.get("Radiacao (KJ/m²)")),
            "precipitation": limpar_valor(row.get("Chuva (mm)"))
        }
        
        # --- AQUI ESTÁ A MUDANÇA: ENVIA PARA OS DOIS ---
        api_ok = enviar_para_api(payload)
        tb_ok = enviar_para_thingsboard(payload)
        
        # Cria visualização do status no terminal
        status_api = "✅ API" if api_ok else "❌ API"
        status_tb = "✅ TB" if tb_ok else "❌ TB"
        
        print(f"[{status_api} | {status_tb}] Temp: {payload['temperature']}°C")
        
        time.sleep(DELAY_SEGUNDOS)

except FileNotFoundError:
    print(f"❌ Erro: O arquivo '{CSV_FILE}' não foi encontrado.")
except Exception as e:
    print(f"❌ Erro inesperado: {e}")